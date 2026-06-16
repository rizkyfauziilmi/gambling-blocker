from __future__ import annotations

import json
import pickle
from hashlib import md5
from io import BytesIO
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

import joblib
import numpy as np
import tensorflow as tf
from keras import Model
from sklearn.ensemble import RandomForestClassifier
from sklearn.preprocessing import StandardScaler

from .config import SAVE_DIR
from .helpers import clean_url
from .logger import log
from .settings import get as get_settings
from .storage import get_storage

NOISE_SIZE_LIMIT: int = 100_000

_text_model: Model | None = None
_vectorizer: Any | None = None
_text_threshold: float = 0.5

_image_model: RandomForestClassifier | None = None
_image_scaler: StandardScaler | None = None
_image_threshold: float = 0.5
_image_alpha: float = 0.4

PATCH_SIZE: tuple[int, int] = (16, 16)
MAX_PATCHES: int = 10
RESIZE: tuple[int, int] = (64, 64)


def load() -> bool:
    global _text_model, _vectorizer, _text_threshold
    global _image_model, _image_scaler, _image_threshold, _image_alpha

    text_model_path: Path = SAVE_DIR / "text_classifier.keras"
    vectorizer_path: Path = SAVE_DIR / "text_tfidf_vectorizer.pkl"
    text_threshold_path: Path = SAVE_DIR / "text_best_threshold.json"
    image_model_path: Path = SAVE_DIR / "image_classifier.pkl"
    image_scaler_path: Path = SAVE_DIR / "image_scaler.pkl"
    fusion_config_path: Path = SAVE_DIR / "image_fusion_alpha.json"

    if not all(
        p.exists()
        for p in [
            text_model_path,
            vectorizer_path,
            text_threshold_path,
            image_model_path,
            image_scaler_path,
            fusion_config_path,
        ]
    ):
        log("WARN", f"Model files not found in {SAVE_DIR} - inference will return 503")
        return False

    _text_model = tf.keras.models.load_model(str(text_model_path))
    with open(vectorizer_path, "rb") as f:
        _vectorizer = pickle.load(f)
    with open(text_threshold_path) as f:
        _text_threshold = json.load(f)["text_best_threshold"]

    _image_model = joblib.load(str(image_model_path))
    _image_scaler = joblib.load(str(image_scaler_path))
    with open(fusion_config_path) as f:
        fc: dict[str, Any] = json.load(f)
        _image_alpha = fc["alpha"]
        _image_threshold = fc["threshold"]

    log(
        "INFO",
        f"Models loaded from {SAVE_DIR} (text_threshold={_text_threshold:.4f}, image_threshold={_image_threshold:.4f}, fusion_alpha={_image_alpha:.2f})",  # noqa: E501
    )
    return True


def is_loaded() -> bool:
    return (
        _text_model is not None
        and _vectorizer is not None
        and _image_model is not None
        and _image_scaler is not None
    )


# ---- Fusion ----


def _extract_features(img_bytes: bytes) -> np.ndarray:
    from matplotlib.colors import rgb_to_hsv
    from PIL import Image
    from scipy.ndimage import sobel
    from sklearn.feature_extraction.image import extract_patches_2d

    img = Image.open(BytesIO(img_bytes)).convert("RGB")
    img_resized = img.resize(RESIZE, Image.LANCZOS)
    arr = np.array(img_resized, dtype=np.uint8)
    arr_float = arr / 255.0

    # 1. Patch features (10 patches x 3 channel x 2 stats = 60 dim)
    patches = extract_patches_2d(arr, patch_size=PATCH_SIZE, max_patches=MAX_PATCHES)
    patch_feat = np.array(
        [float(p[:, :, c].mean()) for p in patches for c in range(3)]
        + [float(p[:, :, c].std()) for p in patches for c in range(3)]
    )

    # 2. Edge density (3 dim)
    gray = np.array(img_resized.convert("L"), dtype=np.float64)
    edges_x = sobel(gray, axis=1)
    edges_y = sobel(gray, axis=0)
    edges_mag = np.hypot(edges_x, edges_y)
    max_mag = edges_mag.max()
    if max_mag > 0:
        edge_feat = np.array(
            [
                edges_mag.mean() / max_mag,
                edges_mag.std() / max_mag,
                (edges_mag > edges_mag.mean()).sum() / edges_mag.size,
            ]
        )
    else:
        edge_feat = np.zeros(3)

    # 3. Color variance 4x4 grid (3 dim)
    h_cells, w_cells = 4, 4
    cell_h, cell_w = arr.shape[0] // h_cells, arr.shape[1] // w_cells
    cell_means = np.zeros((h_cells * w_cells, 3))
    idx = 0
    for i in range(h_cells):
        for j in range(w_cells):
            cell = arr[i * cell_h : (i + 1) * cell_h, j * cell_w : (j + 1) * cell_w]
            cell_means[idx] = cell.mean(axis=(0, 1))
            idx += 1
    color_var = cell_means.std(axis=0)

    # 4. Colorfulness index Hasler & Susstrunk (1 dim)
    r, g, b = (
        arr[:, :, 0].astype(float),
        arr[:, :, 1].astype(float),
        arr[:, :, 2].astype(float),
    )
    rg = r - g
    yb = 0.5 * (r + g) - b
    colorfulness = np.array(
        [
            np.sqrt(rg.std() ** 2 + yb.std() ** 2)
            + 0.3 * np.sqrt(rg.mean() ** 2 + yb.mean() ** 2)
        ]
    )

    # 5. Brightness distribution: dark_ratio, bright_ratio (2 dim)
    hsv = rgb_to_hsv(arr_float)
    v_ch = hsv[:, :, 2]
    dark_ratio = (v_ch < 0.2).sum() / v_ch.size
    bright_ratio = (v_ch > 0.8).sum() / v_ch.size
    brightness_dist = np.array([dark_ratio, bright_ratio])

    return np.concatenate(
        [
            patch_feat,
            edge_feat,
            color_var,
            colorfulness,
            brightness_dist,
        ]
    )


def _is_blocked(page) -> tuple[bool, str]:
    title = page.title().strip()
    if title.lower() in _BLOCKED_TITLES:
        return True, f"blocked_title: {title[:60]}"
    try:
        body = page.text_content("body")
    except Exception:
        return True, "no_body"
    if body:
        body_lower = body.lower()
        for frag in _BLOCKED_BODY_FRAGMENTS:
            if frag in body_lower:
                return True, f"blocked_body: {frag}"
    return False, ""


_BLOCKED_TITLES: set[str] = {
    "just a moment",
    "attention required",
    "access denied",
    "403 forbidden",
    "access denied.",
    "blocked",
    "please wait...",
    "please wait",
    "checking your browser",
    "akses ditolak",
    "halaman tidak ditemukan",
    "terjadi kesalahan",
    "situs ini diblokir",
    "access to this site is blocked",
    "website ini diblokir",
}

_BLOCKED_BODY_FRAGMENTS: set[str] = {
    "checking your browser",
    "ddos protection",
    "please enable cookies",
    "cf-browser-verification",
    "cloudflare",
    "attention required",
    "access denied",
    "403 forbidden",
    "your request has been blocked",
    "sorry, you have been blocked",
    "akses ditolak",
    "situs ini diblokir",
    "website ini diblokir",
    "halaman ini diblokir",
    "koneksi tidak aman",
    "akses diblokir",
    "access to this site has been blocked",
    "situs ini tidak dapat diakses",
    "pemblokiran",
    "ditutup atas perintah",
}

_OVERLAY_REMOVER: str = """
    (() => {
        const selectors = [
            '.modal', '.popup', '.overlay', '.cookie', '.cookies',
            '[class*="cookie"]', '[id*="cookie"]', '.gdpr', '.consent',
            '[class*="consent"]', '[class*="notification"]',
            '.notification-bar', '.adsbox', '.ad-container',
            '[class*="ad-"]', '[id*="ad-"]', '.interstitial',
            '.newsletter-popup', '.email-popup', '.subscribe-popup',
            '[class*="popup"]', '[id*="popup"]', '.fb-lightbox',
            '.modal-backdrop', '.modal-overlay',
        ];
        selectors.forEach(sel => {
            document.querySelectorAll(sel).forEach(el => el.remove());
        });
        document.body.style.overflow = 'visible';
        document.body.style.position = 'static';
    })();
"""


def _capture_screenshot(url: str) -> tuple[bytes | None, str | None]:
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    try:
        with sync_playwright() as pw:
            browser = pw.chromium.launch(
                headless=True,
                args=[
                    "--disable-blink-features=AutomationControlled",
                    "--no-sandbox",
                    "--disable-dev-shm-usage",
                    "--disable-web-security",
                    "--disable-features=IsolateOrigins,site-per-process",
                    "--disable-setuid-sandbox",
                    "--disable-accelerated-2d-canvas",
                    "--disable-gpu",
                    "--disable-notifications",
                    "--disable-geolocation",
                    "--disable-popup-blocking",
                ],
            )
            page = browser.new_page(
                viewport={"width": 1280, "height": 720},
                user_agent=(
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
                locale="id-ID",
                timezone_id="Asia/Jakarta",
                bypass_csp=True,
                ignore_https_errors=True,
                extra_http_headers={
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
                    "Accept-Language": "id-ID,id;q=0.9,en-US,en;q=0.8",
                },
            )
            Stealth().apply_stealth_sync(page)

            page.on("dialog", lambda d: d.dismiss())

            resp = page.goto(url, timeout=30_000, wait_until="domcontentloaded")
            try:
                page.wait_for_load_state("networkidle", timeout=15_000)
            except Exception:
                pass

            blocked, reason = _is_blocked(page)
            if blocked:
                log("SCREENSHOT", f"BLOCKED {url} reason={reason}")
                browser.close()
                return None, "blocked"

            try:
                page.evaluate(_OVERLAY_REMOVER)
                page.wait_for_timeout(500)
            except Exception:
                pass

            http_status: str | None = str(resp.status) if resp else None
            buf: bytes = page.screenshot(full_page=False)

            if len(buf) < 1024:
                log("SCREENSHOT", f"BLANK {url} size={len(buf)}B")
                browser.close()
                return None, "blank"

            log(
                "SCREENSHOT",
                f"{url} status={http_status} size={len(buf) / 1024:.0f}KB",
            )
            browser.close()
        return buf, http_status
    except Exception as e:
        log("WARN", f"Screenshot failed for {url}: {type(e).__name__}: {e}")
        return None, None


def _http_status_label(status: str | None) -> str:
    if status is None:
        return "capture_failed"
    if status == "200":
        return "screenshot_ok"
    if status == "blocked":
        return "blocked"
    if status == "blank":
        return "blank_screenshot"
    return f"http_error_{status}"


def _infer_multipage(url: str, prob_root: float) -> float:
    """Infer root URL + internal subpaths untuk akurasi lebih baik.
    Hanya jalan untuk root domain (path kosong).
    Sampling: path depth >= 2, stratified per directory, max 8.
    Agregasi: average subpaths (tanpa root).
    Fallback ke prob_root jika fetch gagal atau tak ada path.
    """
    import re
    from collections import defaultdict
    from urllib.parse import urljoin, urlparse

    import requests

    parsed = urlparse(url)
    if parsed.path not in ("", "/"):
        return prob_root

    try:
        resp = requests.get(
            url,
            timeout=5,
            headers={
                "User-Agent": (
                    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                    "AppleWebKit/537.36 (KHTML, like Gecko) "
                    "Chrome/125.0.0.0 Safari/537.36"
                ),
            },
        )
        resp.raise_for_status()
    except Exception as e:
        log("MULTIPAGE", f"fetch failed for {url}: {e}")
        return prob_root

    seen: set[str] = set()
    raw_paths: list[str] = []
    for href in re.findall(r'href="([^"]*)"', resp.text):
        full = urljoin(url, href)
        p_url = urlparse(full)
        if p_url.netloc != parsed.netloc:
            continue
        p = p_url.path
        if not p or p == "/":
            continue
        if re.search(
            r"\.(jpg|jpeg|png|gif|svg|webp|ico|css|js|woff2?|ttf|eot|pdf|zip|xml)$",
            p,
            re.I,
        ):
            continue
        if p not in seen:
            seen.add(p)
            raw_paths.append(p)

    if not raw_paths:
        log("MULTIPAGE", f"no internal paths found for {url}")
        return prob_root

    # Pisahkan berdasarkan depth
    deep = [p for p in raw_paths if len([s for s in p.strip("/").split("/") if s]) >= 2]
    shallow = [p for p in raw_paths if p not in deep]

    sampled: list[str] = []
    if len(deep) >= 3:
        # Stratified: max 2 per directory pertama
        groups: defaultdict[str, list[str]] = defaultdict(list)
        for p in deep:
            first_dir = p.strip("/").split("/")[0]
            groups[first_dir].append(p)
        for g in sorted(groups):
            sampled.extend(groups[g][:2])
        sampled = sampled[:8]
    else:
        # Fallback: include depth 1 juga
        sampled = (deep + shallow)[:8]

    if not sampled:
        log("MULTIPAGE", f"no sampled paths for {url}")
        return prob_root

    scores: list[float] = []
    for p in sampled:
        full = urljoin(url, p)
        cleaned = clean_url(full)
        seq = _vectorizer.transform([cleaned])
        seq.sort_indices()
        prob = float(_text_model.predict(seq, verbose=0)[0][0])
        scores.append(prob)
        depth = len([s for s in p.strip("/").split("/") if s])
        log("MULTIPAGE", f"depth={depth} {p} → {prob:.4f}")

    from math import prod

    avg = prod(max(s, 1e-8) for s in scores) ** (1 / len(scores))
    log(
        "MULTIPAGE",
        f"root={prob_root:.4f} sampled={sampled} scores={[round(s, 4) for s in scores]} avg={avg:.4f}",  # noqa: E501
    )
    return avg


def infer_fused(url: str) -> dict[str, Any]:
    assert _vectorizer is not None
    assert _text_model is not None
    assert _image_model is not None
    assert _image_scaler is not None

    # Multipage inference untuk akurasi lebih baik (terutama root domain)
    parsed = urlparse(url)
    if parsed.path in ("", "/"):
        cleaned = clean_url(url)
        seq_tfidf = _vectorizer.transform([cleaned])
        seq_tfidf.sort_indices()
        prob_text = float(_text_model.predict(seq_tfidf, verbose=0)[0][0])
        if get_settings()["multipage_enabled"]:
            prob_text = _infer_multipage(url, prob_text)
    else:
        cleaned = clean_url(url)
        seq_tfidf = _vectorizer.transform([cleaned])
        seq_tfidf.sort_indices()
        prob_text = float(_text_model.predict(seq_tfidf, verbose=0)[0][0])

    # Skip screenshot jika text model sudah konklusif
    if get_settings()["bypass_text_enabled"] and (
        prob_text >= 0.95 or prob_text <= 0.05
    ):
        return {
            "url": url,
            "category": "gambling" if prob_text > _image_threshold else "non-gambling",
            "gambling_score": round(prob_text, 4),
            "text_score": round(prob_text, 4),
            "image_score": None,
            "fusion_alpha": _image_alpha,
            "screenshot_url": None,
            "screenshot_object_key": None,
            "screenshot_status": "bypass_text_only",
        }

    img_bytes, http_status = _capture_screenshot(url)
    prob_image: float | None = None
    screenshot_url: str | None = None
    screenshot_object_key: str | None = None
    screenshot_status: str = "no_screenshot"

    if img_bytes is not None:
        label = _http_status_label(http_status)
        if len(img_bytes) < NOISE_SIZE_LIMIT:
            screenshot_status = (
                f"{label}_noise" if label != "screenshot_ok" else "noise_screenshot"
            )
        else:
            try:
                feats = _extract_features(img_bytes).reshape(1, -1)
                feats_scaled = _image_scaler.transform(feats)
                prob_image = float(_image_model.predict_proba(feats_scaled)[0, 1])

                storage = get_storage()
                object_name: str = f"{md5(url.encode()).hexdigest()[:12]}.png"
                if storage.upload_bytes(object_name, img_bytes):
                    screenshot_url = storage.presigned_url(object_name)
                    screenshot_object_key = object_name
                else:
                    screenshot_object_key = None

                screenshot_status = label
            except Exception as e:
                log("WARN", f"Feature extraction failed: {e}")
                screenshot_status = "extraction_failed"
    else:
        if http_status == "blocked":
            screenshot_status = "blocked"
        elif http_status == "blank":
            screenshot_status = "blank_screenshot"
        else:
            screenshot_status = "capture_failed"

    if prob_image is not None:
        prob_gambling: float = (
            _image_alpha * prob_text + (1 - _image_alpha) * prob_image
        )
    else:
        prob_gambling = prob_text

    category: str = "gambling" if prob_gambling > _image_threshold else "non-gambling"

    return {
        "url": url,
        "category": category,
        "gambling_score": round(prob_gambling, 4),
        "text_score": round(prob_text, 4),
        "image_score": round(prob_image, 4) if prob_image is not None else None,
        "fusion_alpha": _image_alpha,
        "screenshot_url": screenshot_url,
        "screenshot_object_key": screenshot_object_key,
        "screenshot_status": screenshot_status,
    }


_text_model_loaded: bool = load()
