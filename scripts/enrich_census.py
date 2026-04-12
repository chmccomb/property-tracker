"""
US Census ACS Neighborhood Enrichment
---------------------------------------
Pulls American Community Survey (ACS) 5-year estimates for each ZIP code
(ZCTA) found in Testing.csv and writes zip_census.csv.

No API key required — the Census Bureau provides free public access.

Variables fetched per ZIP code:
  B19013_001E — Median household income
  B25002_003E — Vacant housing units
  B25002_001E — Total housing units  (→ vacancy rate = B25002_003E / B25002_001E)
  B25003_003E — Renter-occupied units
  B25003_001E — Occupied units       (→ renter rate = B25003_003E / B25003_001E)
  B01003_001E — Total population

These feed a composite `neighborhood_demand_score` signal in jc_heights_emerging.py:
  high income + low vacancy + moderate-to-high renter % = strong rental demand

Output:
    zip_census.csv — read by jc_heights_emerging.py

Run from data/processed/:
    python ../../scripts/enrich_census.py               # current year (tries latest)
    python ../../scripts/enrich_census.py --year 2022   # specific ACS year
"""

import argparse
import csv
import json
import sys
import urllib.request
import urllib.parse
from pathlib import Path

INPUT_CSV  = "Testing.csv"
OUTPUT_CSV = "zip_census.csv"

# ACS 5-year variables to fetch
ACS_VARS = [
    "B19013_001E",  # Median household income
    "B25002_003E",  # Vacant units
    "B25002_001E",  # Total housing units
    "B25003_003E",  # Renter-occupied
    "B25003_001E",  # Occupied units
    "B01003_001E",  # Total population
]

# The most recent published ACS 5-year dataset (update annually)
DEFAULT_ACS_YEAR = 2022


def fetch_acs(year: int, zctas: list) -> list:
    """Fetch ACS 5-year estimates for the given ZCTAs. Returns list of dicts."""
    # Query specific ZCTAs directly — ZCTAs are a national geography, not
    # nested under states, so &in=state:34 is not valid.
    var_str = ",".join(ACS_VARS)
    zcta_list = ",".join(zctas)
    url = (
        f"https://api.census.gov/data/{year}/acs/acs5"
        f"?get={var_str}&for=zip+code+tabulation+area:{zcta_list}"
    )
    print(f"Fetching Census ACS {year} for NJ ZCTAs …")
    print(f"  URL: {url}")
    try:
        with urllib.request.urlopen(url, timeout=30) as resp:
            data = json.loads(resp.read().decode())
    except Exception as exc:
        print(f"ERROR: Census API request failed: {exc}")
        sys.exit(1)

    # First row is headers
    headers = data[0]
    zcta_idx = headers.index("zip code tabulation area")
    target_set = set(zctas)

    results = []
    for row in data[1:]:
        zcta = row[zcta_idx]
        if zcta not in target_set:
            continue
        record = {"zip": zcta}
        for var in ACS_VARS:
            val_str = row[headers.index(var)]
            try:
                record[var] = float(val_str) if val_str not in (None, "-1", "-666666666") else None
            except (ValueError, TypeError):
                record[var] = None
        results.append(record)

    return results


def compute_neighborhood_demand(record: dict):
    """
    Composite neighborhood demand score [0, 100].

    Components:
      income_score   (40%) — relative median income; scaled at $60k = 50, $120k = 100
      low_vacancy    (30%) — inverted vacancy rate; 0% vacant = 100, 10%+ = 0
      renter_demand  (30%) — renter rate in 40–70% range signals high demand rental market;
                             above 70% or below 30% both score lower
    """
    income  = record.get("B19013_001E")
    vacant  = record.get("B25002_003E")
    total_h = record.get("B25002_001E")
    renters = record.get("B25003_003E")
    occ_h   = record.get("B25003_001E")

    scores = []

    if income is not None and income > 0:
        # $60k → 50, $120k → 100, linear; clamp [0, 100]
        income_score = min(100, max(0, (income - 30_000) / 90_000 * 100))
        scores.append(("income", income_score, 0.40))

    if vacant is not None and total_h is not None and total_h > 0:
        vac_rate = vacant / total_h
        # 0% vacant → 100, 10%+ → 0
        low_vac_score = min(100, max(0, (1 - vac_rate / 0.10) * 100))
        scores.append(("vacancy", low_vac_score, 0.30))

    if renters is not None and occ_h is not None and occ_h > 0:
        renter_rate = renters / occ_h
        # Peak demand signal at 55% renter rate; falls off toward 0% and 100%
        renter_score = min(100, max(0, 100 - abs(renter_rate - 0.55) / 0.45 * 100))
        scores.append(("renter", renter_score, 0.30))

    if not scores:
        return None

    total_w = sum(w for _, _, w in scores)
    composite = sum(s * w for _, s, w in scores) / total_w
    return round(composite, 1)


def main() -> None:
    parser = argparse.ArgumentParser(description="Fetch Census ACS data for JC Heights ZIPs.")
    parser.add_argument("--year", type=int, default=DEFAULT_ACS_YEAR,
                        help=f"ACS 5-year dataset year (default: {DEFAULT_ACS_YEAR})")
    args = parser.parse_args()

    # ── Read ZIPs from Testing.csv ────────────────────────────────────────
    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        raw = list(csv.DictReader(f))

    # Normalize to 5-digit ZIPs (strip ZIP+4 suffixes like "07307-1234")
    zctas = sorted(set(
        row.get("Zip", "").strip()[:5]
        for row in raw
        if len(row.get("Zip", "").strip()) >= 5
    ))
    print(f"ZIPs found in MLS data: {zctas}")

    # ── Fetch from Census API ─────────────────────────────────────────────
    records = fetch_acs(args.year, zctas)
    print(f"Census records returned: {len(records)} of {len(zctas)} ZCTAs requested")

    if not records:
        print("WARNING: No matching Census records found.")
        print("  Verify that the ZIPs in Testing.csv are valid NJ ZCTAs.")
        print("  Some JC ZIPs (e.g. 07302) may not have ZCTA data — check")
        print("  https://www.census.gov/programs-surveys/geography/guidance/geo-areas/zctas.html")
        sys.exit(1)

    # ── Compute composite demand score and write output ───────────────────
    for record in records:
        record["neighborhood_demand_score"] = compute_neighborhood_demand(record)
        record["acs_year"] = args.year
        # Human-readable derived fields
        if record.get("B25002_001E") and record.get("B25002_001E", 0) > 0:
            record["vacancy_rate_pct"] = round(
                (record.get("B25002_003E") or 0) / record["B25002_001E"] * 100, 1)
        else:
            record["vacancy_rate_pct"] = None
        if record.get("B25003_001E") and record.get("B25003_001E", 0) > 0:
            record["renter_rate_pct"] = round(
                (record.get("B25003_003E") or 0) / record["B25003_001E"] * 100, 1)
        else:
            record["renter_rate_pct"] = None

    fieldnames = [
        "zip", "acs_year",
        "B19013_001E", "B25002_003E", "B25002_001E", "B25003_003E", "B25003_001E", "B01003_001E",
        "vacancy_rate_pct", "renter_rate_pct", "neighborhood_demand_score",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(records)

    print(f"\nWrote {len(records)} rows → {OUTPUT_CSV}")
    print("\nNeighborhood demand scores by ZIP:")
    for r in sorted(records, key=lambda x: x.get("neighborhood_demand_score") or 0, reverse=True):
        print(f"  {r['zip']}: score={r.get('neighborhood_demand_score')}  "
              f"income=${r.get('B19013_001E') or 'N/A':,}  "
              f"vacancy={r.get('vacancy_rate_pct')}%  "
              f"renter={r.get('renter_rate_pct')}%")


if __name__ == "__main__":
    main()
