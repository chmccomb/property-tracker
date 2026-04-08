"""
Walk Score Enrichment
---------------------
For each unique address in Testing.csv, fetches Walk Score, Transit Score,
and Bike Score from the Walk Score API. Results are cached in
address_walkscore.csv so subsequent runs only fetch addresses that are new.

Requirements:
    pip install requests

Setup:
    1. Get a free API key at https://www.walkscore.com/professional/api.php
    2. Set the environment variable:
           export WALKSCORE_API_KEY=your_key_here
       or pass it on the command line:
           WALKSCORE_API_KEY=your_key python enrich_walkscore.py

Output:
    address_walkscore.csv — joined into the pipeline by jc_heights_clean.py

Run from data/processed/:
    python ../../scripts/enrich_walkscore.py
"""

import csv
import os
import sys
import time
import urllib.request
import urllib.parse
import json
from pathlib import Path

API_KEY    = os.environ.get("WALKSCORE_API_KEY", "")
INPUT_CSV  = "Testing.csv"
OUTPUT_CSV = "address_walkscore.csv"
BASE_URL   = "https://api.walkscore.com/score/json"

SLEEP_BETWEEN = 0.3  # seconds between calls — generous rate limit, but be polite


def parse_float(s):
    try:
        return float(s.strip()) if s and s.strip() else None
    except (ValueError, AttributeError):
        return None


def fetch_score(address: str, lat: float, lon: float) -> dict:
    params = {
        "format":   "json",
        "address":  address,
        "lat":      lat,
        "lon":      lon,
        "wsapikey": API_KEY,
        "transit":  1,
        "bike":     1,
    }
    url = BASE_URL + "?" + urllib.parse.urlencode(params)
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            d = json.loads(resp.read().decode())
        walk     = d.get("walkscore")
        transit  = d.get("transit", {}).get("score") if isinstance(d.get("transit"), dict) else None
        bike     = d.get("bike", {}).get("score")    if isinstance(d.get("bike"), dict)    else None
        desc     = d.get("description", "")
        return {"address": address, "walk_score": walk, "transit_score": transit,
                "bike_score": bike, "ws_description": desc}
    except Exception as exc:
        print(f"  Warning: Walk Score fetch failed for {address!r}: {exc}")
        return {"address": address, "walk_score": None, "transit_score": None,
                "bike_score": None, "ws_description": "error"}


def main() -> None:
    if not API_KEY:
        print("ERROR: WALKSCORE_API_KEY environment variable not set.")
        print("  Get a free key at https://www.walkscore.com/professional/api.php")
        sys.exit(1)

    # ── Load cache ────────────────────────────────────────────────────────
    cached: dict = {}
    out_path = Path(OUTPUT_CSV)
    if out_path.exists():
        with open(out_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cached[row["address"]] = row
        print(f"Cached Walk Scores loaded: {len(cached)} addresses")

    # ── Read unique addresses + coordinates from Testing.csv ─────────────
    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        raw = list(csv.DictReader(f))

    addresses: dict = {}
    for row in raw:
        addr = row.get("Address", "").strip()
        lat  = parse_float(row.get("Y Coordinates", ""))
        lon  = parse_float(row.get("X Coordinates", ""))
        if addr and addr not in addresses and lat and lon:
            addresses[addr] = (lat, lon)

    new_addresses = [a for a in addresses if a not in cached]
    print(f"Unique addresses: {len(addresses)}  —  new to fetch: {len(new_addresses)}")

    # ── Fetch new addresses ───────────────────────────────────────────────
    results = dict(cached)
    for idx, addr in enumerate(new_addresses, 1):
        lat, lon = addresses[addr]
        score = fetch_score(addr, lat, lon)
        results[addr] = score
        print(f"  [{idx}/{len(new_addresses)}] {addr}: "
              f"walk={score['walk_score']}  transit={score['transit_score']}  bike={score['bike_score']}")
        time.sleep(SLEEP_BETWEEN)

    # ── Write output ──────────────────────────────────────────────────────
    fieldnames = ["address", "walk_score", "transit_score", "bike_score", "ws_description"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in results.values():
            writer.writerow({k: row.get(k, "") for k in fieldnames})

    print(f"\nWrote {len(results)} rows → {OUTPUT_CSV}")
    if new_addresses:
        print("Re-run jc_heights_clean.py to incorporate the new scores.")


if __name__ == "__main__":
    main()
