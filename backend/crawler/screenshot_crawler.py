import asyncio
import os
import time
from datetime import datetime
from pathlib import Path
from typing import cast

import pandas as pd
from pandas import DataFrame
from playwright.async_api import ViewportSize, async_playwright
from playwright_stealth import Stealth
from tqdm import tqdm

DATA_PATH = "model/dataset/dataset_crawl_070526.csv"
SCREENSHOT_DIR = "model/screenshots"
LOG_PATH = "model/screenshots/screenshots_log.csv"
CONCURRENCY = 15
TIMEOUT = 30_000
LOG_FLUSH_EVERY = 100
VIEWPORT = cast(ViewportSize, {"width": 1280, "height": 720})

BLOCKED_TITLES = {
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

BLOCKED_BODY_FRAGMENTS = {
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

OVERLAY_REMOVER = """
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

_shutdown = False


def load_dataset():
    df = pd.read_csv(DATA_PATH)
    df["Webpage_id"] = df["Webpage_id"].astype(str)
    return df


MARKER_EXTS = {".png", ".blocked", ".blank", ".error"}


def get_completed_ids():
    completed = set()
    for tag_dir in ("gambling", "non-gambling"):
        path = os.path.join(SCREENSHOT_DIR, tag_dir)
        if not os.path.isdir(path):
            continue
        for fname in os.listdir(path):
            stem, ext = os.path.splitext(fname)
            if ext in MARKER_EXTS:
                completed.add(stem)
    return completed


def append_log(records: list[dict]):
    if not records:
        return
    dirpath = os.path.dirname(LOG_PATH)
    os.makedirs(dirpath, exist_ok=True)
    new_df = pd.DataFrame(records)
    if os.path.isfile(LOG_PATH):
        new_df.to_csv(LOG_PATH, mode="a", header=False, index=False)
    else:
        new_df.to_csv(LOG_PATH, index=False)


async def is_blocked(page) -> tuple[bool, str]:
    title = (await page.title()).strip()
    if title.lower() in BLOCKED_TITLES:
        return True, f"blocked_title: {title[:60]}"
    try:
        body = await page.text_content("body")
    except Exception:
        return True, "no_body"
    if body:
        body_lower = body.lower()
        for frag in BLOCKED_BODY_FRAGMENTS:
            if frag in body_lower:
                return True, f"blocked_body: {frag}"
    return False, ""


def write_marker(tag_dir: str, webpage_id: str, status: str):
    Path(os.path.join(tag_dir, f"{webpage_id}.{status}")).touch()


def screenshot_path(tag_dir: str, webpage_id: str) -> str:
    return os.path.join(tag_dir, f"{webpage_id}.png")


def make_record(webpage_id: str, url: str, tag: str) -> dict:
    return {
        "webpage_id": webpage_id,
        "url": url,
        "tag": tag,
        "status": "error",
        "error": "",
        "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
    }


async def process_one(context, row) -> dict | None:
    webpage_id = str(row["Webpage_id"])
    tag = row["Tag"]
    tag_dir = os.path.join(SCREENSHOT_DIR, tag)
    os.makedirs(tag_dir, exist_ok=True)

    png = screenshot_path(tag_dir, webpage_id)
    if os.path.isfile(png):
        return None

    record = make_record(webpage_id, row["Url"], tag)

    page = await context.new_page()
    await Stealth().apply_stealth_async(page)
    page.on("dialog", lambda dialog: asyncio.create_task(dialog.dismiss()))

    try:
        await page.goto(row["Url"], timeout=TIMEOUT, wait_until="domcontentloaded")
        try:
            await page.wait_for_load_state("networkidle", timeout=15000)
        except Exception:
            pass

        blocked, reason = await is_blocked(page)
        if blocked:
            record["status"] = "blocked"
            record["error"] = reason
            write_marker(tag_dir, webpage_id, "blocked")
            return record

        await page.evaluate(OVERLAY_REMOVER)
        await page.wait_for_timeout(500)

        await page.screenshot(path=png, full_page=False)

        if os.path.getsize(png) < 1024:
            os.remove(png)
            record["status"] = "blank"
            record["error"] = "screenshot_too_small"
            write_marker(tag_dir, webpage_id, "blank")
            return record

        record["status"] = "success"

    except asyncio.CancelledError:
        if os.path.isfile(png):
            os.remove(png)
        raise

    except Exception as exc:
        record["status"] = "error"
        record["error"] = str(exc)[:200]
        if os.path.isfile(png):
            os.remove(png)
        write_marker(tag_dir, webpage_id, "error")

    finally:
        await page.close()

    return record


async def worker(worker_id: int, queue: asyncio.Queue, browser, pbar: tqdm):
    context = await browser.new_context(
        viewport=VIEWPORT,
        user_agent=(
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/125.0.0.0 Safari/537.36"
        ),
        locale="id-ID",
        timezone_id="Asia/Jakarta",
        bypass_csp=True,
        ignore_https_errors=True,
        java_script_enabled=True,
        extra_http_headers={
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "id-ID,id;q=0.9,en-US,en;q=0.8",
        },
    )

    records: list[dict] = []

    try:
        while not _shutdown:
            try:
                row = await queue.get()
            except asyncio.CancelledError:
                break

            if row is None:
                queue.task_done()
                break

            result = await process_one(context, row)
            if result is not None:
                records.append(result)

            pbar.update(1)

            if len(records) >= LOG_FLUSH_EVERY:
                append_log(records)
                records.clear()

            queue.task_done()
    except asyncio.CancelledError:
        pass
    finally:
        if records:
            append_log(records)
        await context.close()

    return records


async def main():
    global _shutdown

    df = load_dataset()
    completed = get_completed_ids()
    remaining = cast(DataFrame, df.loc[~df["Webpage_id"].isin(list(completed))])
    total = len(remaining)

    if total == 0:
        print("Semua URL sudah memiliki screenshot.")
        return

    print(f"Total: {len(df)}, sudah: {len(completed)}, sisa: {total}")

    queue: asyncio.Queue = asyncio.Queue(maxsize=CONCURRENCY * 4)

    async with async_playwright() as p:
        browser = await p.chromium.launch(
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
            ],
        )

        with tqdm(total=total, desc="Screenshot", unit="url", ncols=100) as pbar:
            workers_running = [
                asyncio.create_task(worker(i, queue, browser, pbar))
                for i in range(CONCURRENCY)
            ]

            producer = asyncio.create_task(_producer(queue, remaining))

            try:
                await producer
                await queue.join()
            except (KeyboardInterrupt, asyncio.CancelledError):
                _shutdown = True
                print("\nMenghentikan worker...")

            for w in workers_running:
                if w.done() and not w.cancelled():
                    try:
                        await w
                    except (KeyboardInterrupt, asyncio.CancelledError):
                        pass

        await browser.close()


async def _producer(queue: asyncio.Queue, remaining: pd.DataFrame):
    for _, row in remaining.iterrows():
        if _shutdown:
            break
        await queue.put(row)

    for _ in range(CONCURRENCY):
        await queue.put(None)


if __name__ == "__main__":
    start = time.time()
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    elapsed = time.time() - start
    print(f"Waktu: {elapsed / 60:.1f} menit")
