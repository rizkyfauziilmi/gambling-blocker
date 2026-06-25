from __future__ import annotations

import argparse
import base64
import csv
import json
import os
import random
import statistics
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.parse import urlparse

import requests
from tabulate import tabulate

HERE = Path(__file__).resolve().parent
SETTINGS_PATH = HERE.parent / "settings.json"

STATUS_LABELS = {
    "bypass_text_only": "Text Only",
    "screenshot_ok": "Screenshot OK",
    "blocked": "Blocked",
    "blank_screenshot": "Blank",
    "capture_failed": "Capture Failed",
    "extraction_failed": "Extraction Failed",
    "no_screenshot": "No Screenshot",
    "noise_screenshot": "Noise",
}


def read_settings() -> dict:
    try:
        return json.loads(SETTINGS_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_settings(data: dict) -> None:
    current = read_settings()
    current.update(data)
    SETTINGS_PATH.write_text(json.dumps(current, indent=2) + "\n")


def load_urls(path: str, strip_path: bool = False) -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    with open(path, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("url") or row.get("Url") or "").strip()
            raw = row.get("category") or row.get("Tag") or row.get("label") or ""
            cat = raw.strip().lower()
            if not url:
                continue
            if strip_path:
                parsed = urlparse(url)
                root = f"{parsed.scheme}://{parsed.netloc}"
                if root in seen:
                    continue
                seen.add(root)
                rows.append({"url": root, "category": cat})
            else:
                rows.append({"url": url, "category": cat})
    return rows


def flush_cache(api_url: str, auth: str | None) -> None:
    if not auth:
        print("  flush-cache skipped (BENCH_API_AUTH not set)", file=sys.stderr)
        return
    token = base64.b64encode(auth.encode()).decode()
    headers = {"Authorization": f"Basic {token}"}
    try:
        resp = requests.delete(f"{api_url}/cache", headers=headers, timeout=10)
        if resp.ok:
            deleted = resp.json().get("deleted", "?")
            print(f"  cache flushed ({deleted} entries)")
        else:
            print(f"  flush-cache failed: HTTP {resp.status_code}", file=sys.stderr)
    except requests.RequestException as e:
        print(f"  flush-cache error: {e}", file=sys.stderr)


def probe_url(url: str, timeout: float = 5.0) -> bool:
    try:
        r = requests.head(url, timeout=timeout, allow_redirects=True)
        return r.ok
    except requests.RequestException:
        return False


def compute_stats(values: list[float]) -> dict:
    if not values:
        return {
            "count": 0, "mean": 0, "median": 0,
            "min": 0, "max": 0,
        }
    sorted_v = sorted(values)
    n = len(sorted_v)
    return {
        "count": n,
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "min": sorted_v[0],
        "max": sorted_v[-1],
    }


def fmt_table(rows: list[tuple], headers: list[str]) -> str:
    return tabulate(
        rows, headers=headers,
        floatfmt=".3f", numalign="right", stralign="left",
    )


def build_groups(all_results: list[dict]) -> dict:
    groups = {
        "overall": [r["response_time_s"] for r in all_results],
        "cache_hit": [
            r["response_time_s"] for r in all_results if r["from_cache"]
        ],
        "cache_miss": [
            r["response_time_s"] for r in all_results if not r["from_cache"]
        ],
    }

    ss_values: dict[str, list[float]] = defaultdict(list)
    for r in all_results:
        ss = r["screenshot_status"]
        label = STATUS_LABELS.get(ss, ss)
        ss_values[label].append(r["response_time_s"])

    cat_values: dict[str, list[float]] = defaultdict(list)
    for r in all_results:
        cat_values[r["category"]].append(r["response_time_s"])

    return {
        "groups": groups,
        "ss_values": dict(ss_values),
        "cat_values": dict(cat_values),
    }


def save_md_report(path: Path, all_results: list[dict], runs: int) -> None:
    data = build_groups(all_results)
    headers = ["group", "count", "mean (s)", "median (s)", "min (s)", "max (s)"]
    lines: list[str] = [
        "# Response Time Report",
        "",
        f"Runs: {runs}",
        "",
    ]

    def add_section(title: str, rows: list[tuple]) -> None:
        lines.append(f"## {title}")
        lines.append("")
        md = tabulate(
            rows, headers=headers,
            floatfmt=".3f", numalign="right", stralign="left",
            tablefmt="pipe",
        )
        lines.append(md)
        lines.append("")

    rows_groups: list[tuple] = []
    for label in ("cache_hit", "cache_miss", "overall"):
        s = compute_stats(data["groups"][label])
        rows_groups.append((
            label, s["count"], s["mean"], s["median"],
            s["min"], s["max"],
        ))
    add_section("Cache Status", rows_groups)

    rows_ss: list[tuple] = []
    for label in sorted(data["ss_values"]):
        s = compute_stats(data["ss_values"][label])
        rows_ss.append((
            label, s["count"], s["mean"], s["median"],
            s["min"], s["max"],
        ))
    add_section("Screenshot Status", rows_ss)

    rows_cat: list[tuple] = []
    for label in sorted(data["cat_values"]):
        s = compute_stats(data["cat_values"][label])
        rows_cat.append((
            label, s["count"], s["mean"], s["median"],
            s["min"], s["max"],
        ))
    add_section("Category", rows_cat)

    md_path = path.with_suffix(".md")
    md_path.write_text("\n".join(lines) + "\n")
    print(f"Report saved: {md_path}")


def run_benchmark(args: argparse.Namespace) -> None:
    api_url = os.environ.get("BENCH_API_URL")
    if not api_url:
        print("error: BENCH_API_URL environment variable is not set", file=sys.stderr)
        sys.exit(1)
    api_url = api_url.rstrip("/")

    settings = read_settings()
    settings_before = dict(settings)

    if args.bypass_text:
        settings["bypass_text_enabled"] = True
    if args.no_multipage:
        settings["multipage_enabled"] = False
    if settings != settings_before:
        write_settings(settings)
        print(f"[settings] bypass_text={settings.get('bypass_text_enabled')}, "
              f"multipage={settings.get('multipage_enabled')}")
    else:
        bt = settings.get("bypass_text_enabled")
        mp = settings.get("multipage_enabled")
        print(f"[settings] untouched (bypass_text={bt}, multipage={mp})")

    all_urls = load_urls(args.urls, args.strip_path)
    if not all_urls:
        print("error: no URLs loaded", file=sys.stderr)
        sys.exit(1)

    random.shuffle(all_urls)
    cat_pool = defaultdict(list)
    for u in all_urls:
        cat_pool[u["category"]].append(u)

    g = len(cat_pool.get("gambling", []))
    ng = len(cat_pool.get("non-gambling", []))
    print(f"\nTotal URLs in dataset: {len(all_urls)} (gambling={g}, non-gambling={ng})")
    if args.strip_path:
        print("  (strip-path: root domain only)")

    if args.min_per_category:
        urls = []
        needed = {
            c: args.min_per_category
            for c in ("gambling", "non-gambling")
            if c in cat_pool
        }
        random.shuffle(all_urls)
        collected: dict[str, int] = defaultdict(int)
        for u in all_urls:
            c = u["category"]
            if c not in needed or collected[c] >= needed[c]:
                continue
            urls.append(u)
            collected[c] += 1
            if all(collected[c] >= needed[c] for c in needed):
                break
        print(f"Sampled for min-per-category={args.min_per_category}: "
              f"{len(urls)} total "
              f"(gambling={collected.get('gambling', 0)}, "
              f"non-gambling={collected.get('non-gambling', 0)})")
    elif args.sample:
        random.shuffle(all_urls)
        urls = all_urls[:args.sample]
        print(f"Sampled: {len(urls)} URLs")
    else:
        urls = all_urls
        print(f"Using all URLs: {len(urls)}")

    if args.flush_cache:
        auth = os.environ.get("BENCH_API_AUTH")
        flush_cache(api_url, auth)

    if args.probe:
        print("\nProbing URLs (HEAD 5s)...")
        for i, u in enumerate(urls):
            t0 = time.monotonic()
            ok = probe_url(u["url"])
            elapsed = time.monotonic() - t0
            u["reachable"] = ok
            u["probe_time"] = elapsed
            if (i + 1) % 500 == 0:
                print(f"  {i + 1}/{len(urls)} probed")
        reachable = sum(1 for u in urls if u.get("reachable"))
        print(f"  reachable={reachable}, unreachable={len(urls) - reachable}")
    else:
        for u in urls:
            u["reachable"] = True

    all_results: list[dict] = []
    last_request: dict[str, float] = {}
    ss_counts: defaultdict[str, int] = defaultdict(int)

    for run_idx in range(args.runs):
        random.shuffle(urls)
        for i, u in enumerate(urls):
            hostname = urlparse(u["url"]).netloc
            now = time.time()
            since_last = now - last_request.get(hostname, 0)
            if since_last < args.delay:
                wait = args.delay - since_last
                time.sleep(wait)

            t0 = time.monotonic()
            resp = None
            data: dict = {}
            try:
                resp = requests.get(
                    f"{api_url}/classify/url-fused",
                    params={"url": u["url"]},
                    timeout=120,
                )
                data = resp.json() if resp.ok else {}
            except requests.RequestException as e:
                print(f"  request failed: {u['url'][:60]} ({e})", file=sys.stderr)
            elapsed = time.monotonic() - t0
            last_request[hostname] = time.time()

            result = {
                "url": u["url"],
                "category": data.get("category", "error"),
                "response_time_s": round(elapsed, 4),
                "status_code": resp.status_code if resp is not None else 0,
                "from_cache": data.get("from_cache", False),
                "screenshot_status": data.get("screenshot_status", "error"),
                "gambling_score": data.get("gambling_score"),
                "text_score": data.get("text_score"),
                "image_score": data.get("image_score"),
                "reachable": u.get("reachable", True),
                "run": run_idx + 1,
            }
            all_results.append(result)

            if args.min_per_category:
                ss = result["screenshot_status"]
                ss_counts[ss] += 1
                if (i + 1) % 25 == 0:
                    print(f"  [{i + 1}] last: {elapsed:.2f}s | "
                          f"ss: {dict(ss_counts)}")
            elif (i + 1) % 10 == 0:
                print(f"  [{i + 1}/{len(urls)}] last: {elapsed:.2f}s")

    if args.output:
        out_path = Path(args.output)
        fieldnames = [
            "run", "url", "category", "response_time_s", "status_code",
            "from_cache", "screenshot_status", "gambling_score",
            "text_score", "image_score", "reachable",
        ]
        with out_path.open("w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(all_results)
        print(f"\nRaw results saved: {out_path}")

    print("\n" + "=" * 60)
    print(f"  Response Time Report — {args.runs} run(s)")
    print("=" * 60)

    data = build_groups(all_results)
    headers = ["group", "count", "mean (s)", "median (s)", "min (s)", "max (s)"]

    def append_row(rows, values):
        s = compute_stats(values)
        rows.append((
            s["count"], s["mean"], s["median"],
            s["min"], s["max"],
        ))

    print("\nby cache status:")
    rows = []
    for label in ("cache_hit", "cache_miss", "overall"):
        append_row(rows, data["groups"][label])
        rows[-1] = (label,) + rows[-1]
    print(fmt_table(rows, headers))

    print("\nby screenshot_status:")
    rows = []
    for label in sorted(data["ss_values"]):
        append_row(rows, data["ss_values"][label])
        rows[-1] = (label,) + rows[-1]
    print(fmt_table(rows, headers))

    print("\nby category:")
    rows = []
    for label in sorted(data["cat_values"]):
        append_row(rows, data["cat_values"][label])
        rows[-1] = (label,) + rows[-1]
    print(fmt_table(rows, headers))

    if args.output:
        save_md_report(Path(args.output), all_results, args.runs)

    if args.reset:
        write_settings(settings_before)
        print("\n[settings] restored to original values")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark classification API response time",
    )
    parser.add_argument(
        "--urls", required=True,
        help="CSV file with columns: url[,category]",
    )
    parser.add_argument(
        "--delay", type=float, default=7.0,
        help="Delay (s) between requests to same hostname",
    )
    parser.add_argument(
        "--sample", type=int, default=None,
        help="Number of URLs to sample (default: all)",
    )
    parser.add_argument(
        "--min-per-category", type=int, default=None,
        help="Stop after collecting N successful results per category",
    )
    parser.add_argument("--runs", type=int, default=1, help="Repeat URL list N times")
    parser.add_argument(
        "--bypass-text", action="store_true",
        help="Set bypass_text_enabled=true",
    )
    parser.add_argument(
        "--no-multipage", action="store_true",
        help="Set multipage_enabled=false",
    )
    parser.add_argument(
        "--strip-path", action="store_true",
        help="Strip URL path to root domain only",
    )
    parser.add_argument(
        "--flush-cache", action="store_true",
        help="Flush Redis cache before benchmark",
    )
    parser.add_argument(
        "--probe", action="store_true",
        help="Pre-filter reachability via HEAD",
    )
    parser.add_argument("--output", help="Save raw results to CSV + .md report")
    parser.add_argument(
        "--reset", action="store_true",
        help="Restore settings.json after run",
    )
    args = parser.parse_args()

    try:
        run_benchmark(args)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        if args.reset:
            print("run with --reset to restore settings.json", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
