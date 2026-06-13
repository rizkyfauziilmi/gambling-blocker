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
MAX_PATCHES: int = 50
RESIZE: tuple[int, int] = (64, 64)
N_HIST_BINS: int = 32


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
        print(f"[WARN] Model files not found in {SAVE_DIR} - inference will return 503")
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

    print(
        f"[INFO] Models loaded from {SAVE_DIR} "
        f"(text_threshold={_text_threshold:.4f}, "
        f"image_threshold={_image_threshold:.4f}, "
        f"fusion_alpha={_image_alpha:.2f})"
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

    patches = extract_patches_2d(arr, patch_size=PATCH_SIZE, max_patches=MAX_PATCHES)
    patch_feat: list[float] = []
    for p in patches:
        for c in range(3):
            patch_feat.append(float(p[:, :, c].mean()))
            patch_feat.append(float(p[:, :, c].std()))

    hist_feat: list[float] = []
    for c in range(3):
        h = img.histogram()[c * 256 : (c + 1) * 256]
        bin_sz = 256 // N_HIST_BINS
        binned = [sum(h[j * bin_sz : (j + 1) * bin_sz]) for j in range(N_HIST_BINS)]
        total = sum(binned) + 1e-8
        hist_feat.extend(b / total for b in binned)

    arr_float = arr / 255.0
    hsv = rgb_to_hsv(arr_float)
    h_ch, s_ch = hsv[:, :, 0], hsv[:, :, 1]
    h_hist, _ = np.histogram(h_ch, bins=N_HIST_BINS, range=(0, 360), density=True)
    s_hist, _ = np.histogram(s_ch, bins=N_HIST_BINS, range=(0, 1), density=True)
    hsv_feat = np.concatenate([h_hist, s_hist])

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

    warm_mask = ((h_ch >= 0) & (h_ch <= 60)) | ((h_ch >= 330) & (h_ch <= 360))
    warm_ratio = np.array([warm_mask.sum() / h_ch.size])

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

    return np.concatenate(
        [patch_feat, hist_feat, hsv_feat, edge_feat, warm_ratio, color_var]
    )


def _capture_screenshot(url: str) -> tuple[bytes | None, str | None]:
    from playwright.sync_api import sync_playwright
    from playwright_stealth import Stealth

    BLOCKED_DOMAINS: set[str] = {
        "doubleclick.net",
        "googlesyndication.com",
        "googletagmanager.com",
        "google-analytics.com",
        "facebook.net",
        "fbcdn.net",
        "amazon-adsystem.com",
        "adservice.google.com",
        "criteo.com",
        "scorecardresearch.com",
        "hotjar.com",
        "newrelic.com",
    }

    INIT_CSS: str = """
        (() => {
            const s = document.createElement('style');
            s.id = '__clean_css';
            s.textContent = `
                [class*="cookie"],[id*="cookie"],[class*="Cookie"],[id*="Cookie"],
                [class*="popup"],[id*="popup"],[class*="Popup"],[id*="Popup"],
                [class*="modal"],[id*="modal"],[class*="Modal"],[id*="Modal"],
                [class*="overlay"],[id*="overlay"],[class*="Overlay"],[id*="Overlay"],
                [class*="consent"],[id*="consent"],[class*="Consent"],[id*="Consent"],
                [class*="notification"],[id*="notification"],[class*="Notification"],[id*="Notification"],
                [class*="gdpr"],[id*="gdpr"],[class*="GDPR"],
                [class*="ad-"],[id*="ad-"],[class*="banner"],[id*="banner"],
                [class*="backdrop"],[id*="backdrop"],[class*="Backdrop"],[id*="Backdrop"],
                [class*="interstitial"],[aria-modal="true"],[role="dialog"],
                .modal,.popup,.overlay,.cookie,.cookies,.gdpr,.consent,
                .newsletter,.subscribe,.modal-backdrop,.modal-overlay,
                .fb-lightbox,.notification-bar,.adsbox,.ad-container
                { display:none!important;visibility:hidden!important;
                  pointer-events:none!important;opacity:0!important;z-index:-1!important }
            `;
            document.documentElement.appendChild(s);
        })();
    """

    REMOVE_OVERLAYS: str = """
        (() => {
            const SEL = [
                '[class*="cookie"],[id*="cookie"],[class*="Cookie"],[id*="Cookie"]',
                '[class*="popup"],[id*="popup"],[class*="Popup"],[id*="Popup"]',
                '[class*="modal"],[id*="modal"],[class*="Modal"],[id*="Modal"]',
                '[class*="overlay"],[id*="overlay"],[class*="Overlay"],[id*="Overlay"]',
                '[class*="consent"],[id*="consent"],[class*="Consent"],[id*="Consent"]',
                '[class*="notification"],[id*="notification"],[class*="Notification"],[id*="Notification"]',
                '[class*="gdpr"],[id*="gdpr"],[class*="GDPR"]',
                '[class*="ad-"],[id*="ad-"],[class*="banner"],[id*="banner"]',
                '[class*="backdrop"],[id*="backdrop"],[class*="Backdrop"],[id*="Backdrop"]',
                '[class*="interstitial"],[aria-modal="true"],[role="dialog"]',
                '.modal,.popup,.overlay,.cookie,.cookies,.gdpr,.consent',
                '.newsletter,.subscribe,.modal-backdrop,.modal-overlay',
                '.fb-lightbox,.notification-bar,.adsbox,.ad-container',
            ].join(',');
            function deep(root) {
                root.querySelectorAll(SEL).forEach(e => e.remove());
                root.querySelectorAll('*').forEach(e => {
                    if (e.shadowRoot) deep(e.shadowRoot);
                });
            }
            deep(document);
            const all = document.querySelectorAll('*');
            const vw = innerWidth, vh = innerHeight;
            all.forEach(el => {
                if (el === document.body || el === document.documentElement) return;
                const s = getComputedStyle(el);
                if (s.position !== 'fixed' && s.position !== 'sticky') return;
                const z = parseInt(s.zIndex);
                if (isNaN(z) || z < 100) return;
                const r = el.getBoundingClientRect();
                if (r.width * r.height > vw * vh * 0.3) return;
                el.remove();
            });
            all.forEach(el => {
                if (el === document.body || el === document.documentElement) return;
                const s = getComputedStyle(el);
                if (s.position !== 'fixed' && s.position !== 'absolute') return;
                const z = parseInt(s.zIndex);
                if (isNaN(z) || z < 50) return;
                const r = el.getBoundingClientRect();
                if (r.width * r.height > vw * vh * 0.5) el.remove();
            });
            all.forEach(el => {
                const s = getComputedStyle(el);
                if (parseFloat(s.opacity) > 0.3) return;
                if (s.position !== 'fixed' && s.position !== 'absolute') return;
                const r = el.getBoundingClientRect();
                if (r.width * r.height > vw * vh * 0.9) el.remove();
            });
            ['body','html'].forEach(tag => {
                const el = document.querySelector(tag);
                if (!el) return;
                el.style.overflow = 'visible';
                el.style.position = 'static';
            });
        })();
    """

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
            )
            Stealth().apply_stealth_sync(page)

            page.on("dialog", lambda d: d.dismiss())

            page.route("**/*", lambda route: (
                route.abort()
                if any(d in route.request.url for d in BLOCKED_DOMAINS)
                else route.continue_()
            ))

            page.add_init_script(INIT_CSS)

            resp = page.goto(url, timeout=7000, wait_until="domcontentloaded")

            try:
                page.evaluate(REMOVE_OVERLAYS)
                page.keyboard.press("Escape")
                page.mouse.click(5, 5)
            except Exception:
                pass

            http_status: str | None = str(resp.status) if resp else None
            buf: bytes = page.screenshot(full_page=False)
            browser.close()
        return buf, http_status
    except Exception as e:
        print(f"[WARN] Screenshot failed for {url}: {e}")
        return None, None


def _http_status_label(status: str | None) -> str:
    if status is None:
        return "capture_failed"
    if status == "200":
        return "screenshot_ok"
    return f"http_error_{status}"


def _infer_multipage(url: str, prob_root: float) -> float:
    """Infer root URL + up to 5 internal paths, return average score.
    Hanya jalan untuk root domain (path kosong).
    Fallback ke prob_root jika fetch gagal atau tak ada path.
    """
    import re

    import requests
    from urllib.parse import urljoin, urlparse

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
    except Exception:
        return prob_root

    seen: set[str] = set()
    paths: list[str] = []
    for href in re.findall(r'href="([^"]*)"', resp.text):
        if href.startswith("/"):
            p = urlparse(href).path
        else:
            p_url = urlparse(href)
            if p_url.netloc and p_url.netloc != parsed.netloc:
                continue
            p = p_url.path
        if not p or p == "/":
            continue
        if re.search(
            r"\.(jpg|jpeg|png|gif|svg|webp|ico|css|js|woff2?|ttf|eot|pdf|zip|xml)$", p, re.I
        ):
            continue
        if p not in seen:
            seen.add(p)
            paths.append(p)

    if not paths:
        return prob_root

    scores: list[float] = [prob_root]
    for p in sorted(paths)[:5]:
        cleaned = clean_url(urljoin(url, p))
        seq = _vectorizer.transform([cleaned])
        seq.sort_indices()
        prob = float(_text_model.predict(seq, verbose=0)[0][0])
        scores.append(prob)

    return sum(scores) / len(scores)


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
        prob_text = _infer_multipage(url, prob_text)
    else:
        cleaned = clean_url(url)
        seq_tfidf = _vectorizer.transform([cleaned])
        seq_tfidf.sort_indices()
        prob_text = float(_text_model.predict(seq_tfidf, verbose=0)[0][0])

    # Skip screenshot jika text model sudah konklusif — hemat ~7s
    if prob_text >= 0.95 or prob_text <= 0.05:
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
                print(f"[WARN] Feature extraction failed: {e}")
                screenshot_status = "extraction_failed"
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
