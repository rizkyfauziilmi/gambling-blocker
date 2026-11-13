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
from tqdm import tqdm

HERE = Path(__file__).resolve().parent
SETTINGS_PATH = HERE.parent / "settings.json"
OUT_DIR = HERE / "output"
DATASET_PATH = HERE.parent / "model" / "dataset" / "dataset_crawl_070526.csv"

GROUP_LABELS: dict[str, str] = {
    "bypass_text_only": "Teks Saja",
    "bypass_list": "Teks Saja",
    "screenshot_ok": "Screenshot OK",
}

GROUP_ORDER = ["Teks Saja", "Screenshot OK", "Screenshot Gagal"]


def read_settings() -> dict:
    try:
        return json.loads(SETTINGS_PATH.read_text())
    except (FileNotFoundError, json.JSONDecodeError):
        return {}


def write_settings(data: dict) -> None:
    current = read_settings()
    current.update(data)
    SETTINGS_PATH.write_text(json.dumps(current, indent=2) + "\n")


def load_urls() -> list[dict]:
    rows: list[dict] = []
    seen: set[str] = set()
    with open(DATASET_PATH, newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            url = (row.get("url") or row.get("Url") or "").strip()
            raw = row.get("category") or row.get("Tag") or row.get("label") or ""
            cat = raw.strip().lower()
            if not url:
                continue
            parsed = urlparse(url)
            hostname = parsed.hostname
            if not hostname or hostname in seen:
                continue
            seen.add(hostname)
            rows.append({"url": f"{parsed.scheme}://{hostname}", "category": cat})
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


def classify(api_url: str, url: str, timeout: float = 180) -> dict:
    try:
        resp = requests.get(
            f"{api_url}/classify/url-fused",
            params={"url": url},
            timeout=timeout,
        )
        data = resp.json() if resp.ok else {}
        return {
            "status_code": resp.status_code,
            "category": data.get("category", "error"),
            "from_cache": data.get("from_cache", False),
            "screenshot_status": data.get("screenshot_status", "error"),
        }
    except requests.RequestException as e:
        print(f"  request failed: {url[:60]} ({e})", file=sys.stderr)
        return {
            "status_code": 0,
            "category": "error",
            "from_cache": False,
            "screenshot_status": "error",
        }


def status_group(raw: str) -> str:
    if raw in GROUP_LABELS:
        return GROUP_LABELS[raw]
    return "Screenshot Gagal"


def compute_stats(values: list[float]) -> dict:
    if not values:
        return {"count": 0, "mean": 0, "median": 0, "min": 0, "max": 0}
    sorted_v = sorted(values)
    return {
        "count": len(sorted_v),
        "mean": statistics.mean(values),
        "median": statistics.median(values),
        "min": sorted_v[0],
        "max": sorted_v[-1],
    }


def run_phase(
    api_url: str,
    sampled: list[dict],
    phase_label: str,
) -> list[dict]:
    results: list[dict] = []
    random.shuffle(sampled)
    for u in (pbar := tqdm(sampled, desc=phase_label, unit="req", leave=True)):
        t0 = time.monotonic()
        data = classify(api_url, u["url"])
        elapsed = time.monotonic() - t0
        pbar.set_postfix({"last": f"{elapsed:.2f}s"})

        results.append({
            "phase": phase_label,
            "url": u["url"],
            "category": data["category"],
            "response_time_s": round(elapsed, 4),
            "from_cache": data["from_cache"],
            "screenshot_status": data["screenshot_status"],
        })
    return results


def save_csv(path: Path, rows: list[dict]) -> None:
    fieldnames = [
        "phase", "url", "category", "response_time_s",
        "from_cache", "screenshot_status", "condition",
    ]
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"  saved: {path}")


def load_csv(path: Path) -> list[dict]:
    with path.open(newline="") as f:
        return list(csv.DictReader(f))


def build_report_rows(results: list[dict]) -> list[tuple]:
    groups: dict[str, dict[str, list[float]]] = defaultdict(
        lambda: {"cold": [], "warm": []}
    )
    for r in results:
        cond = r.get("condition") or status_group(r["screenshot_status"])
        ph = r["phase"]
        t = float(r["response_time_s"])
        groups[cond][ph].append(t)

    rows = []
    for cond in list(GROUP_ORDER) + ["Multipage"]:
        if cond not in groups:
            continue
        cs = compute_stats(groups[cond].get("cold", []))
        ws = compute_stats(groups[cond].get("warm", []))
        speedup = round(cs["mean"] / ws["mean"], 1) if ws["mean"] > 0 else float("inf")
        rows.append((
            cond,
            cs["count"], cs["mean"], cs["median"], cs["min"], cs["max"],
            ws["count"], ws["mean"], ws["median"], ws["min"], ws["max"],
            f"{speedup:.1f}x",
        ))

    all_cold = [float(r["response_time_s"]) for r in results if r["phase"] == "cold"]
    all_warm = [float(r["response_time_s"]) for r in results if r["phase"] == "warm"]
    acs = compute_stats(all_cold)
    aws = compute_stats(all_warm)
    speedup_all = round(acs["mean"] / aws["mean"], 1) if aws["mean"] > 0 else float("inf")
    rows.append((
        "Overall",
        acs["count"], acs["mean"], acs["median"], acs["min"], acs["max"],
        aws["count"], aws["mean"], aws["median"], aws["min"], aws["max"],
        f"{speedup_all:.1f}x",
    ))
    return rows


def run_fase_a(api_url: str, auth: str | None) -> list[dict]:
    settings = read_settings()
    settings_before = dict(settings)
    settings["bypass_text_enabled"] = True
    settings["multipage_enabled"] = False
    write_settings(settings)
    print("[settings] bypass_text=True, multipage=False")

    all_urls = load_urls()
    g = sum(1 for u in all_urls if u["category"] == "gambling")
    ng = sum(1 for u in all_urls if u["category"] == "non-gambling")
    print(f"  total root domains: {len(all_urls)} (gambling={g}, non-gambling={ng})")

    print("\n--- Flushing cache ---")
    flush_cache(api_url, auth)

    print(f"\n--- Cold: classifying all {len(all_urls)} domains ---")
    cold_results = run_phase(api_url, all_urls, "cold")

    print("\n--- Grouping ---")
    pool: dict[str, list[dict]] = defaultdict(list)
    for r in cold_results:
        pool[status_group(r["screenshot_status"])].append(r)

    for grp in GROUP_ORDER:
        print(f"  {grp}: {len(pool.get(grp, []))} available")

    warm_items = [{"url": r["url"], "category": r["category"]}
                  for r in cold_results]
    random.shuffle(warm_items)

    print(f"\n--- Warm: re-classifying {len(warm_items)} URLs ---")
    warm_results = run_phase(api_url, warm_items, "warm")

    ts = time.strftime("%Y%m%d_%H%M%S")
    for r in cold_results:
        r["condition"] = status_group(r["screenshot_status"])
    for r in warm_results:
        r["condition"] = status_group(r["screenshot_status"])

    all_results = cold_results + warm_results
    csv_path = OUT_DIR / f"benchmark_a_{ts}.csv"
    save_csv(csv_path, all_results)

    rows = build_report_rows(all_results)
    headers = [
        "Kondisi", "n cold", "cold mean", "cold median", "cold min", "cold max",
        "n warm", "warm mean", "warm median", "warm min", "warm max", "speedup",
    ]
    print(f"\n{'=' * 80}")
    print("  Fase A — Optimal")
    print(f"{'=' * 80}")
    print(tabulate(rows, headers=headers, floatfmt=".3f", numalign="right", stralign="left"))

    md_path = csv_path.with_suffix(".md")
    md_table = tabulate(rows, headers=headers, floatfmt=".3f",
                        numalign="right", stralign="left", tablefmt="pipe")
    md_path.write_text(
        "# Benchmark — Fase A (Optimal)\n\n"
        f"Date: {ts}\n"
        f"Dataset: {DATASET_PATH.name}\n\n"
        "## Cold vs Warm\n\n"
        f"{md_table}\n"
    )
    print(f"Report saved: {md_path}")
    write_settings(settings_before)
    print("[settings] restored")

    return all_results


def run_fase_b(api_url: str, auth: str | None) -> None:
    settings = read_settings()
    settings_before = dict(settings)
    settings["bypass_text_enabled"] = False
    settings["multipage_enabled"] = True
    write_settings(settings)
    print("[settings] bypass_text=False, multipage=True")

    all_urls = load_urls()
    g = sum(1 for u in all_urls if u["category"] == "gambling")
    ng = sum(1 for u in all_urls if u["category"] == "non-gambling")
    print(f"  total root domains: {len(all_urls)} (gambling={g}, non-gambling={ng})")

    print("\n--- Flushing cache ---")
    flush_cache(api_url, auth)

    target = 100
    print(f"\n--- Cold: classifying until {target} samples ---")
    cold_results: list[dict] = []
    random.shuffle(all_urls)

    with tqdm(total=target, desc="cold", unit="req") as pbar:
        for u in all_urls:
            if len(cold_results) >= target:
                break
            t0 = time.monotonic()
            data = classify(api_url, u["url"])
            elapsed = time.monotonic() - t0
            pbar.set_postfix({"last": f"{elapsed:.2f}s"})

            cold_results.append({
                "phase": "cold",
                "url": u["url"],
                "category": data["category"],
                "response_time_s": round(elapsed, 4),
                "from_cache": data["from_cache"],
                "screenshot_status": data["screenshot_status"],
            })
            pbar.update(1)

    warm_items = [{"url": r["url"], "category": r["category"]} for r in cold_results]
    random.shuffle(warm_items)

    print(f"\n--- Warm: re-classifying {len(warm_items)} URLs ---")
    warm_results = run_phase(api_url, warm_items, "warm")

    ts = time.strftime("%Y%m%d_%H%M%S")
    for r in cold_results:
        r["condition"] = "Multipage"
    for r in warm_results:
        r["condition"] = "Multipage"

    results_b = cold_results + warm_results
    csv_path_b = OUT_DIR / f"benchmark_b_{ts}.csv"
    save_csv(csv_path_b, results_b)

    a_files = sorted(OUT_DIR.glob("benchmark_a_*.csv"), reverse=True)
    if not a_files:
        print("error: no benchmark_a_*.csv found — run 'make benchmark' first",
              file=sys.stderr)
        write_settings(settings_before)
        sys.exit(1)

    results_a = load_csv(a_files[0])
    print(f"\n--- Merging with {a_files[0].name} ---")

    merged = results_a + results_b
    csv_final = OUT_DIR / f"benchmark_final_{ts}.csv"
    save_csv(csv_final, merged)

    rows = build_report_rows(merged)
    headers = [
        "Kondisi", "n cold", "cold mean", "cold median", "cold min", "cold max",
        "n warm", "warm mean", "warm median", "warm min", "warm max", "speedup",
    ]
    print(f"\n{'=' * 80}")
    print("  Benchmark Final — Semua Kondisi")
    print(f"{'=' * 80}")
    print(tabulate(rows, headers=headers, floatfmt=".3f",
                   numalign="right", stralign="left"))

    md_path = csv_final.with_suffix(".md")
    md_table = tabulate(rows, headers=headers, floatfmt=".3f",
                        numalign="right", stralign="left", tablefmt="pipe")
    md_path.write_text(
        "# Benchmark Final\n\n"
        f"Date: {ts}\n"
        f"Dataset: {DATASET_PATH.name}\n"
        f"Fase A: {a_files[0].name}\n"
        f"Fase B: {csv_path_b.name}\n\n"
        "## Cold vs Warm — Semua Kondisi\n\n"
        f"{md_table}\n"
    )
    print(f"Report saved: {md_path}")

    write_settings(settings_before)
    print("[settings] restored")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Benchmark API response time",
    )
    parser.add_argument(
        "--multipage", action="store_true",
        help="Fase B: bypass_text=false, multipage=true + merge final report",
    )
    args = parser.parse_args()

    api_url = os.environ.get("BENCH_API_URL")
    if not api_url:
        print("error: BENCH_API_URL not set", file=sys.stderr)
        sys.exit(1)
    api_url = api_url.rstrip("/")
    auth = os.environ.get("BENCH_API_AUTH")

    try:
        if args.multipage:
            run_fase_b(api_url, auth)
        else:
            run_fase_a(api_url, auth)
    except KeyboardInterrupt:
        print("\ninterrupted", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
