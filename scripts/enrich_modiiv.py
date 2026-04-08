"""
NJ MOD-IV Property Tax Enrichment
-----------------------------------
Joins NJ MOD-IV annual property tax records to MLS data by (block, lot),
filling in official assessed values where the MLS field is missing and
computing a current-year assessment ratio for every property.

MOD-IV improves on the MLS "Assessed Value" field because:
  • MLS records the assessed value at *time of sale* (may be stale)
  • MOD-IV is updated each year by the municipal tax assessor
  • This gives a clean assessment-lag signal: properties where the market
    price has raced ahead of assessments are often undervalued by buyers
    who anchor on tax comps.

How to get MOD-IV data (free):
    1. Go to https://modiv.rutgers.edu  (requires free registration)
       OR https://njogis-newjersey.opendata.arcgis.com/
    2. Download the Jersey City / Hudson County MOD-IV file for the
       current year as CSV.  The file is typically named something like
       "HudsonMODIV_2024.csv" or exported from the ArcGIS layer.
    3. Place it at data/raw/modiiv_hudson.csv  (relative to project root)
       OR set MODIIV_CSV env var to the full path.

Output:
    block_modiiv.csv — joined into the pipeline by jc_heights_clean.py

Key output fields per (block, lot):
    modiiv_assessed   — current total assessed value ($)
    modiiv_land       — land portion of assessment ($)
    modiiv_impr       — improvement (building) portion ($)
    modiiv_year       — assessment year
    modiiv_class      — NJ property class code (2 = residential condo/house)

Run from data/processed/:
    python ../../scripts/enrich_modiiv.py
    python ../../scripts/enrich_modiiv.py --csv /path/to/HudsonMODIV_2024.csv
"""

import argparse
import csv
import os
import re
import sys
from pathlib import Path

# Common column name variants across MOD-IV vintages / export tools
_COL_ALIASES = {
    "block":   ["BLOCK", "Block", "block", "PAMS_PIN_Block"],
    "lot":     ["LOT", "Lot", "lot", "PAMS_PIN_Lot"],
    "qual":    ["QUALIFIER", "Qualifier", "qualifier", "QUAL"],
    "assessed":["TOTAL_ASSESSED_VALUE", "Total_Assessed_Value", "assessed_value",
                "ASSESSED_VALUE", "TOTAL_ASSESS"],
    "land":    ["LAND_VALUE", "Land_Value", "ASSESSED_LAND", "LAND"],
    "impr":    ["IMPROVEMENT_VALUE", "Improvement_Value", "ASSESSED_IMPR", "IMPR"],
    "class":   ["PROPERTY_CLASS", "Property_Class", "CLASS", "PROP_CLASS"],
    "year":    ["MOD4_YEAR", "Mod4_Year", "YEAR", "ASSESS_YEAR"],
}

OUTPUT_CSV = "block_modiiv.csv"


def find_col(headers: list, key: str) -> str | None:
    for alias in _COL_ALIASES.get(key, []):
        if alias in headers:
            return alias
    return None


def clean_num(s: str) -> float | None:
    if not s or not s.strip():
        return None
    cleaned = re.sub(r"[^\d.]", "", s.strip())
    try:
        v = float(cleaned)
        return v if v >= 0 else None
    except ValueError:
        return None


def main() -> None:
    parser = argparse.ArgumentParser(description="Build block_modiiv.csv from NJ MOD-IV data.")
    parser.add_argument(
        "--csv",
        default=os.environ.get("MODIIV_CSV", "../../data/raw/modiiv_hudson.csv"),
        help="Path to MOD-IV CSV (default: ../../data/raw/modiiv_hudson.csv)",
    )
    args = parser.parse_args()

    modiiv_path = Path(args.csv)
    if not modiiv_path.exists():
        print(f"ERROR: MOD-IV CSV not found at {modiiv_path}")
        print()
        print("Download instructions:")
        print("  1. Register (free) at https://modiv.rutgers.edu")
        print("  2. Download the Hudson County MOD-IV CSV for the current year")
        print("  3. Save to data/raw/modiiv_hudson.csv  (or pass --csv PATH)")
        sys.exit(1)

    print(f"Reading {modiiv_path} …")
    with open(modiiv_path, encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        headers = reader.fieldnames or []
        rows = list(reader)

    print(f"  {len(rows):,} MOD-IV rows loaded")
    print(f"  Columns: {headers[:10]} …")

    # Map column names
    cols = {k: find_col(headers, k) for k in _COL_ALIASES}
    missing = [k for k, v in cols.items() if v is None and k not in ("qual", "year")]
    if missing:
        print(f"WARNING: Could not find columns for: {missing}")
        print("  Available columns:", headers)
        print("  The script will continue but those fields will be empty.")

    # Filter to residential (class 2 = res condo/house, class 4C = condo)
    res_classes = {"2", "4C", "4A", "2A", "2B"}
    out_rows: dict = {}  # key: (block, lot) → best record

    skipped_class = 0
    for row in rows:
        prop_class = (row.get(cols["class"], "") or "").strip() if cols["class"] else ""
        if prop_class and prop_class not in res_classes:
            skipped_class += 1
            continue

        block = (row.get(cols["block"], "") or "").strip() if cols["block"] else ""
        lot   = (row.get(cols["lot"],   "") or "").strip() if cols["lot"]   else ""
        if not block or not lot:
            continue

        # Normalize: remove leading zeros so they match MLS "Block"/"Lot" fields
        block_norm = str(int(block)) if block.isdigit() else block
        lot_norm   = str(int(lot))   if lot.isdigit()   else lot

        assessed = clean_num(row.get(cols["assessed"], "") if cols["assessed"] else "")
        land     = clean_num(row.get(cols["land"],     "") if cols["land"]     else "")
        impr     = clean_num(row.get(cols["impr"],     "") if cols["impr"]     else "")
        year_raw = (row.get(cols["year"], "") if cols["year"] else "").strip()

        key = (block_norm, lot_norm)
        # Prefer the record with the highest assessment year (most current)
        existing = out_rows.get(key)
        if existing and year_raw and existing.get("modiiv_year", "") >= year_raw:
            continue

        out_rows[key] = {
            "block_raw":    block_norm,
            "lot":          lot_norm,
            "modiiv_assessed": assessed,
            "modiiv_land":     land,
            "modiiv_impr":     impr,
            "modiiv_class":    prop_class,
            "modiiv_year":     year_raw,
        }

    print(f"  Skipped non-residential: {skipped_class:,}")
    print(f"  Residential (block, lot) pairs: {len(out_rows):,}")

    if not out_rows:
        print("ERROR: No residential records found. Check property class column and values.")
        sys.exit(1)

    fieldnames = ["block_raw", "lot", "modiiv_assessed", "modiiv_land",
                  "modiiv_impr", "modiiv_class", "modiiv_year"]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(out_rows.values())

    print(f"\nWrote {len(out_rows):,} rows → {OUTPUT_CSV}")
    print("Re-run jc_heights_clean.py to incorporate the updated assessments.")


if __name__ == "__main__":
    main()
