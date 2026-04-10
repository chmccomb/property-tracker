#!/usr/bin/env python3
"""
JC Heights — Data Refresh Pipeline
------------------------------------
Runs the full pipeline (clean → emerging → embed) whenever a new
Paragon MLS CSV export is available.

Usage:
    python refresh.py --csv path/to/new_export.csv
    python refresh.py --csv path/to/new_export.csv --watch    # re-run on file change
    python refresh.py --csv path/to/new_export.csv --enrich   # run enrichment scripts first
    python refresh.py --csv path/to/new_export.csv --deploy   # git push after refresh

Steps:
    1. (Optional, --enrich) Run enrich_walkscore.py, enrich_census.py
    2. Copy the input CSV to data/processed/Testing.csv
    3. Run jc_heights_clean.py    (produces cleaned CSVs)
    4. Run jc_heights_emerging.py (produces dashboard_data_v2.json)
    5. Embed the new JSON blob into dashboard/jc_heights_dashboard_v2.html
    6. (Optional, --deploy) Copy dashboard to docs/index.html, git add/commit/push
"""

import argparse
import json
import os
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT         = Path(__file__).resolve().parent
SCRIPTS_DIR  = ROOT / "scripts"
DATA_DIR     = ROOT / "data" / "processed"
DASHBOARD    = ROOT / "dashboard" / "jc_heights_dashboard_v2.html"

# Scripts expect to run from DATA_DIR (all intermediate files live there)
CLEAN_SCRIPT      = SCRIPTS_DIR / "jc_heights_clean.py"
EMERGING_SCRIPT   = SCRIPTS_DIR / "jc_heights_emerging.py"
WALKSCORE_SCRIPT  = SCRIPTS_DIR / "enrich_walkscore.py"
CENSUS_SCRIPT     = SCRIPTS_DIR / "enrich_census.py"
MODIIV_SCRIPT     = SCRIPTS_DIR / "enrich_modiiv.py"
GEOCODE_SCRIPT    = SCRIPTS_DIR / "geocode.py"
TRANSIT_SCRIPT    = SCRIPTS_DIR / "enrich_transit.py"
INPUT_CSV_DEST    = DATA_DIR / "Testing.csv"
OUTPUT_JSON       = DATA_DIR / "dashboard_data_v2.json"
DOCS_DIR          = ROOT / "docs"
DOCS_DASHBOARD    = DOCS_DIR / "index.html"


def run_step(label: str, cmd: list, cwd: Path) -> None:
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    result = subprocess.run(
        cmd, cwd=str(cwd), capture_output=False, text=True
    )
    if result.returncode != 0:
        print(f"\n✗ {label} failed (exit {result.returncode})", file=sys.stderr)
        sys.exit(result.returncode)
    print(f"✓ {label} complete")


def embed_json_in_dashboard(json_path: Path, html_path: Path) -> None:
    """Replace the `const D = {...};` line in the dashboard HTML with fresh JSON."""
    print(f"\n{'─'*60}")
    print("  Embedding JSON into dashboard HTML")
    print(f"{'─'*60}")

    with open(json_path, "r", encoding="utf-8") as f:
        raw_json = f.read().strip()

    # Validate it's real JSON before touching the HTML
    try:
        json.loads(raw_json)
    except json.JSONDecodeError as e:
        print(f"✗ Generated JSON is invalid: {e}", file=sys.stderr)
        sys.exit(1)

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    # Replace the single `const D = {...};` line (the JSON is always one line
    # because emerging.py serializes with separators=(',',':'))
    new_line = f"const D = {raw_json};"
    updated, n = re.subn(r"^const D = \{.*\};$", new_line, html, flags=re.MULTILINE)

    if n != 1:
        print(f"✗ Expected to replace 1 `const D = ...` line, found {n}", file=sys.stderr)
        sys.exit(1)

    # Write a backup before overwriting
    backup = html_path.with_suffix(".html.bak")
    shutil.copy2(html_path, backup)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(updated)

    size_kb = html_path.stat().st_size / 1024
    print(f"✓ Dashboard updated ({size_kb:.0f} KB) — backup at {backup.name}")


def run_enrichment() -> None:
    """Run optional enrichment scripts (Geocode, Walk Score, Census ACS)."""
    print(f"\n{'='*60}")
    print("  Running enrichment scripts")
    print(f"{'='*60}")

    import os
    ws_key = os.environ.get("WALKSCORE_API_KEY", "")
    if ws_key:
        run_step(
            "Walk Score enrichment",
            [sys.executable, str(WALKSCORE_SCRIPT)],
            cwd=DATA_DIR,
        )
    else:
        print("  ⚠ Skipping Walk Score (WALKSCORE_API_KEY not set)")

    run_step(
        "Census ACS enrichment",
        [sys.executable, str(CENSUS_SCRIPT)],
        cwd=DATA_DIR,
    )

    run_step(
        "MOD-IV assessment enrichment",
        [sys.executable, str(MODIIV_SCRIPT)],
        cwd=DATA_DIR,
    )


def deploy_to_github(timestamp: str) -> None:
    """Copy dashboard to docs/index.html and push to GitHub Pages."""
    print(f"\n{'─'*60}")
    print("  Deploying to GitHub Pages")
    print(f"{'─'*60}")

    import shutil as _shutil
    DOCS_DIR.mkdir(exist_ok=True)
    _shutil.copy2(DASHBOARD, DOCS_DASHBOARD)
    size_kb = DOCS_DASHBOARD.stat().st_size / 1024
    print(f"✓ Copied dashboard → docs/index.html ({size_kb:.0f} KB)")

    import subprocess as _sp

    def _git(args: list, **kw) -> int:
        result = _sp.run(["git"] + args, cwd=str(ROOT), capture_output=True, text=True, **kw)
        if result.stdout.strip():
            print(f"  {result.stdout.strip()}")
        if result.stderr.strip() and result.returncode != 0:
            print(f"  {result.stderr.strip()}", file=sys.stderr)
        return result.returncode

    _git(["add", "docs/index.html", "dashboard/jc_heights_dashboard_v2.html"])
    commit_msg = f"dashboard: refresh data {timestamp}"
    rc = _git(["commit", "-m", commit_msg])
    if rc == 0:
        push_rc = _git(["push"])
        if push_rc == 0:
            print("✓ Pushed to GitHub — dashboard live at your GitHub Pages URL")
        else:
            print("✗ git push failed — check remote configuration", file=sys.stderr)
    else:
        print("  Nothing to commit (dashboard unchanged)")


def merge_incremental(base_path: Path, incremental_path: Path, dest: Path) -> int:
    """
    Merge incremental CSV (e.g. gmail rows) into the base Paragon export.
    Base rows are kept in full; incremental rows whose MLS # is not already
    present in the base are appended.  Returns the number of new rows added.
    """
    import csv as _csv

    with open(base_path, encoding="utf-8-sig") as f:
        base_rows = list(_csv.DictReader(f))
    base_mls = {r.get("MLS #", "").strip() for r in base_rows if r.get("MLS #", "").strip()}
    fieldnames = list(base_rows[0].keys()) if base_rows else []

    with open(incremental_path, encoding="utf-8-sig") as f:
        inc_rows = list(_csv.DictReader(f))

    # Strip internal gmail fields before merging
    new_rows = []
    for row in inc_rows:
        mls = row.get("MLS #", "").strip()
        if mls and mls not in base_mls:
            # Add any missing columns as empty strings
            merged = {k: "" for k in fieldnames}
            merged.update({k: v for k, v in row.items() if k in fieldnames})
            new_rows.append(merged)
            base_mls.add(mls)

    with open(dest, "w", newline="", encoding="utf-8") as f:
        writer = _csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(base_rows)
        writer.writerows(new_rows)

    return len(new_rows)


def refresh(csv_path: Path, enrich: bool = False, deploy: bool = False,
            incremental: Path = None) -> None:
    if not csv_path.exists():
        print(f"✗ CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*60}")
    print(f"  JC Heights Refresh — {ts}")
    print(f"  Input: {csv_path}")
    print(f"{'='*60}")

    # 1. Stage input CSV where clean.py expects it (must happen before enrichment
    #    so enrich_census.py can read Testing.csv to discover which ZIPs to pull)
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(csv_path, INPUT_CSV_DEST)
    print(f"\n✓ Copied {csv_path.name} → {INPUT_CSV_DEST.relative_to(ROOT)}")

    # 1b. (Optional) Merge incremental gmail rows on top of the base export
    if incremental:
        if not incremental.exists():
            print(f"✗ Incremental CSV not found: {incremental}", file=sys.stderr)
            sys.exit(1)
        n = merge_incremental(csv_path, incremental, INPUT_CSV_DEST)
        print(f"✓ Merged {n} new rows from {incremental.name} into Testing.csv")

    # 2. Geocode new addresses (always runs — instant for cached addresses)
    run_step(
        "Geocoding new addresses",
        [sys.executable, str(GEOCODE_SCRIPT)],
        cwd=DATA_DIR,
    )

    # 2a. Transit proximity (always runs — pure math, no API, instant)
    run_step(
        "Transit proximity",
        [sys.executable, str(TRANSIT_SCRIPT)],
        cwd=DATA_DIR,
    )

    # 2b. (Optional) Run heavier enrichment scripts (Walk Score, Census ACS)
    if enrich:
        run_enrichment()

    # 3. Clean + score
    run_step(
        "Step 1 — jc_heights_clean.py",
        [sys.executable, str(CLEAN_SCRIPT)],
        cwd=DATA_DIR,
    )

    # 4. Emerging scores + JSON build
    run_step(
        "Step 2 — jc_heights_emerging.py",
        [sys.executable, str(EMERGING_SCRIPT)],
        cwd=DATA_DIR,
    )

    # 5. Embed into dashboard HTML
    embed_json_in_dashboard(OUTPUT_JSON, DASHBOARD)

    # 6. (Optional) Deploy to GitHub Pages
    if deploy:
        deploy_to_github(ts)

    print(f"\n{'='*60}")
    print(f"  ✓ Refresh complete — open dashboard/jc_heights_dashboard_v2.html")
    print(f"{'='*60}\n")


def watch(csv_path: Path, interval: int = 5,
          enrich: bool = False, deploy: bool = False) -> None:
    """Poll the CSV file for changes (mtime) and re-run the pipeline when it updates."""
    try:
        import hashlib

        def file_hash(p: Path) -> str:
            return hashlib.md5(p.read_bytes()).hexdigest()

        print(f"\nWatching {csv_path} for changes (checking every {interval}s) …")
        print("Press Ctrl+C to stop.\n")

        last_hash = file_hash(csv_path) if csv_path.exists() else None
        refresh(csv_path, enrich=enrich, deploy=deploy)  # run once immediately

        while True:
            time.sleep(interval)
            if not csv_path.exists():
                continue
            current_hash = file_hash(csv_path)
            if current_hash != last_hash:
                print(f"\n● Change detected in {csv_path.name}")
                last_hash = current_hash
                refresh(csv_path, enrich=enrich, deploy=deploy)
    except KeyboardInterrupt:
        print("\nWatcher stopped.")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh JC Heights dashboard from a new Paragon MLS CSV export."
    )
    parser.add_argument(
        "--csv",
        required=True,
        type=Path,
        help="Path to the new Paragon MLS export CSV",
    )
    parser.add_argument(
        "--watch",
        action="store_true",
        help="Keep running and re-trigger whenever the CSV file changes",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=5,
        help="Poll interval in seconds when --watch is set (default: 5)",
    )
    parser.add_argument(
        "--enrich",
        action="store_true",
        help="Run enrichment scripts (Walk Score, Census ACS) before the main pipeline",
    )
    parser.add_argument(
        "--deploy",
        action="store_true",
        help="After refresh, copy dashboard to docs/index.html and git push (GitHub Pages)",
    )
    parser.add_argument(
        "--incremental",
        type=Path,
        default=None,
        help="Incremental CSV (e.g. from gmail_ingest.py) to merge on top of --csv base export",
    )
    args = parser.parse_args()

    if args.watch:
        watch(args.csv, interval=args.interval, enrich=args.enrich, deploy=args.deploy)
    else:
        refresh(args.csv, enrich=args.enrich, deploy=args.deploy, incremental=args.incremental)


if __name__ == "__main__":
    main()
