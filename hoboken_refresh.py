#!/usr/bin/env python3
"""
Hoboken — Data Refresh Pipeline
---------------------------------
Runs the full Hoboken pipeline whenever a new Paragon MLS CSV export is
available.  Mirrors refresh.py but wired to the Hoboken scripts and data dir.

Usage:
    python hoboken_refresh.py --csv path/to/new_export.csv
    python hoboken_refresh.py --csv path/to/new_export.csv --enrich   # re-run MOD-IV
    python hoboken_refresh.py --csv path/to/new_export.csv --deploy   # git push after

Steps:
    1. Copy the input CSV to data/processed/hoboken/Testing.csv
    2. Run geocode.py --city ", Hoboken, NJ"  (instant for cached addresses)
    3. Run enrich_transit.py                  (pure math, instant)
    4. (Optional, --enrich) Run enrich_modiiv.py --muni 0910
    5. Run hoboken_clean.py
    6. Run hoboken_emerging.py
    7. Embed the new JSON into dashboard/hoboken_dashboard.html
    8. (Optional, --deploy) Copy to docs/hoboken.html, git add/commit/push
"""

import argparse
import json
import re
import shutil
import subprocess
import sys
import time
from pathlib import Path

# ── Paths ──────────────────────────────────────────────────────────────────
ROOT          = Path(__file__).resolve().parent
SCRIPTS_DIR   = ROOT / "scripts"
DATA_DIR      = ROOT / "data" / "processed" / "hoboken"
DASHBOARD     = ROOT / "dashboard" / "hoboken_dashboard.html"

CLEAN_SCRIPT    = SCRIPTS_DIR / "hoboken_clean.py"
EMERGING_SCRIPT = SCRIPTS_DIR / "hoboken_emerging.py"
WALKSCORE_SCRIPT = SCRIPTS_DIR / "enrich_walkscore.py"
CENSUS_SCRIPT   = SCRIPTS_DIR / "enrich_census.py"
MODIIV_SCRIPT   = SCRIPTS_DIR / "enrich_modiiv.py"
GEOCODE_SCRIPT  = SCRIPTS_DIR / "geocode.py"
TRANSIT_SCRIPT  = SCRIPTS_DIR / "enrich_transit.py"

INPUT_CSV_DEST  = DATA_DIR / "Testing.csv"
OUTPUT_JSON     = DATA_DIR / "hoboken_dashboard_data.json"
DOCS_DIR        = ROOT / "docs"
DOCS_DASHBOARD  = DOCS_DIR / "hoboken.html"


def run_step(label: str, cmd: list, cwd: Path) -> None:
    print(f"\n{'─'*60}")
    print(f"  {label}")
    print(f"{'─'*60}")
    result = subprocess.run(cmd, cwd=str(cwd), capture_output=False, text=True)
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

    try:
        json.loads(raw_json)
    except json.JSONDecodeError as e:
        print(f"✗ Generated JSON is invalid: {e}", file=sys.stderr)
        sys.exit(1)

    with open(html_path, "r", encoding="utf-8") as f:
        html = f.read()

    new_line = f"const D = {raw_json};"
    updated, n = re.subn(r"^const D = \{.*\};$", new_line, html, flags=re.MULTILINE)

    if n != 1:
        print(f"✗ Expected to replace 1 `const D = ...` line, found {n}", file=sys.stderr)
        sys.exit(1)

    backup = html_path.with_suffix(".html.bak")
    shutil.copy2(html_path, backup)

    with open(html_path, "w", encoding="utf-8") as f:
        f.write(updated)

    size_kb = html_path.stat().st_size / 1024
    print(f"✓ Dashboard updated ({size_kb:.0f} KB) — backup at {backup.name}")


def run_enrichment() -> None:
    """Run optional enrichment scripts."""
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
        "MOD-IV assessment enrichment (Hoboken)",
        [sys.executable, str(MODIIV_SCRIPT), "--muni", "0905"],
        cwd=DATA_DIR,
    )


def deploy_to_github(timestamp: str) -> None:
    """Copy dashboard to docs/hoboken.html and push to GitHub Pages."""
    print(f"\n{'─'*60}")
    print("  Deploying to GitHub Pages")
    print(f"{'─'*60}")

    DOCS_DIR.mkdir(exist_ok=True)
    shutil.copy2(DASHBOARD, DOCS_DASHBOARD)
    size_kb = DOCS_DASHBOARD.stat().st_size / 1024
    print(f"✓ Copied dashboard → docs/hoboken.html ({size_kb:.0f} KB)")

    def _git(args: list) -> int:
        result = subprocess.run(
            ["git"] + args, cwd=str(ROOT), capture_output=True, text=True
        )
        if result.stdout.strip():
            print(f"  {result.stdout.strip()}")
        if result.stderr.strip() and result.returncode != 0:
            print(f"  {result.stderr.strip()}", file=sys.stderr)
        return result.returncode

    _git(["add", "docs/hoboken.html", "dashboard/hoboken_dashboard.html"])
    commit_msg = f"hoboken: refresh data {timestamp}"
    rc = _git(["commit", "-m", commit_msg])
    if rc == 0:
        push_rc = _git(["push"])
        if push_rc == 0:
            print("✓ Pushed to GitHub — Hoboken dashboard live")
        else:
            print("✗ git push failed — check remote configuration", file=sys.stderr)
    else:
        print("  Nothing to commit (dashboard unchanged)")


def refresh(csv_path: Path, enrich: bool = False, deploy: bool = False) -> None:
    if not csv_path.exists():
        print(f"✗ CSV not found: {csv_path}", file=sys.stderr)
        sys.exit(1)

    ts = time.strftime('%Y-%m-%d %H:%M:%S')
    print(f"\n{'='*60}")
    print(f"  Hoboken Refresh — {ts}")
    print(f"  Input: {csv_path}")
    print(f"{'='*60}")

    DATA_DIR.mkdir(parents=True, exist_ok=True)
    shutil.copy2(csv_path, INPUT_CSV_DEST)
    print(f"\n✓ Copied {csv_path.name} → {INPUT_CSV_DEST.relative_to(ROOT)}")

    # Geocode new addresses (instant for cached addresses)
    run_step(
        "Geocoding new addresses",
        [sys.executable, str(GEOCODE_SCRIPT), "--city", ", Hoboken, NJ"],
        cwd=DATA_DIR,
    )

    # Transit proximity (pure math, instant)
    run_step(
        "Transit proximity",
        [sys.executable, str(TRANSIT_SCRIPT)],
        cwd=DATA_DIR,
    )

    # Optional heavier enrichment
    if enrich:
        run_enrichment()

    # Clean + score
    run_step(
        "Step 1 — hoboken_clean.py",
        [sys.executable, str(CLEAN_SCRIPT)],
        cwd=DATA_DIR,
    )

    # Emerging scores + JSON
    run_step(
        "Step 2 — hoboken_emerging.py",
        [sys.executable, str(EMERGING_SCRIPT)],
        cwd=DATA_DIR,
    )

    # Embed into dashboard HTML
    embed_json_in_dashboard(OUTPUT_JSON, DASHBOARD)

    if deploy:
        deploy_to_github(ts)

    print(f"\n{'='*60}")
    print(f"  ✓ Refresh complete — open dashboard/hoboken_dashboard.html")
    print(f"{'='*60}\n")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Refresh Hoboken dashboard from a new Paragon MLS CSV export."
    )
    parser.add_argument("--csv", required=True, type=Path,
                        help="Path to the new Paragon MLS export CSV")
    parser.add_argument("--enrich", action="store_true",
                        help="Re-run enrichment scripts (MOD-IV, Walk Score, Census ACS)")
    parser.add_argument("--deploy", action="store_true",
                        help="After refresh, copy to docs/hoboken.html and git push")
    args = parser.parse_args()
    refresh(args.csv, enrich=args.enrich, deploy=args.deploy)


if __name__ == "__main__":
    main()
