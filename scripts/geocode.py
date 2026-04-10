"""
Address Geocoder — Nominatim / OpenStreetMap
---------------------------------------------
Geocodes every unique address in Testing.csv using the free Nominatim API
(no key required). Results are cached in address_geocoded.csv so subsequent
runs only fetch addresses that are new.

Usage policy: Nominatim requires a descriptive User-Agent and max 1 req/sec.
This script respects both. Do not reduce SLEEP_BETWEEN below 1.0.

Output:
    address_geocoded.csv — lat/lon per address, read by:
        • enrich_walkscore.py  (Walk Score API needs coordinates)
        • jc_heights_clean.py  (populates lat/lon in cleaned output)

Run from data/processed/:
    python ../../scripts/geocode.py
    python ../../scripts/geocode.py --limit 50   # test with first 50 addresses
"""

import argparse
import csv
import json
import time
import urllib.request
import urllib.parse
from pathlib import Path

INPUT_CSV   = "Testing.csv"
OUTPUT_CSV  = "address_geocoded.csv"
SLEEP_BETWEEN = 1.05   # Nominatim policy: max 1 req/sec
USER_AGENT  = "jc-heights-analysis/1.0 (property research tool)"
BASE_URL    = "https://nominatim.openstreetmap.org/search"

DEFAULT_CITY_SUFFIX = ", Jersey City, NJ"


def geocode(address: str, city_suffix: str = DEFAULT_CITY_SUFFIX) -> dict:
    """Geocode a single address. Returns dict with lat, lon, display_name."""
    query = address + city_suffix
    params = urllib.parse.urlencode({
        "q":      query,
        "format": "json",
        "limit":  1,
    })
    url = f"{BASE_URL}?{params}"
    req = urllib.request.Request(url, headers={"User-Agent": USER_AGENT})
    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            results = json.loads(resp.read().decode())
        if results:
            r = results[0]
            return {
                "address":      address,
                "lat":          float(r["lat"]),
                "lon":          float(r["lon"]),
                "display_name": r.get("display_name", ""),
                "status":       "ok",
            }
        else:
            return {"address": address, "lat": None, "lon": None,
                    "display_name": "", "status": "not_found"}
    except Exception as exc:
        return {"address": address, "lat": None, "lon": None,
                "display_name": "", "status": f"error: {exc}"}


def main() -> None:
    parser = argparse.ArgumentParser(description="Geocode MLS addresses via Nominatim.")
    parser.add_argument("--limit", type=int, default=None,
                        help="Only geocode first N new addresses (for testing)")
    parser.add_argument("--city", default=DEFAULT_CITY_SUFFIX,
                        help=f"City suffix appended to each address (default: '{DEFAULT_CITY_SUFFIX}')")
    args = parser.parse_args()
    city_suffix = args.city if args.city.startswith(",") else f", {args.city}"

    # ── Load existing cache ───────────────────────────────────────────────────
    cache: dict = {}
    out_path = Path(OUTPUT_CSV)
    if out_path.exists():
        with open(out_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                cache[row["address"]] = row
        print(f"Cached geocodes loaded: {len(cache)} addresses")

    # ── Read unique addresses from Testing.csv ────────────────────────────────
    with open(INPUT_CSV, encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))

    unique_addresses = sorted({
        r.get("Address", "").strip()
        for r in rows
        if r.get("Address", "").strip()
    })
    print(f"Unique addresses in MLS data: {len(unique_addresses)}")

    new_addresses = [a for a in unique_addresses if a not in cache]
    if args.limit:
        new_addresses = new_addresses[:args.limit]

    print(f"New addresses to geocode: {len(new_addresses)}")
    if not new_addresses:
        print("Nothing to fetch — cache is up to date.")
        return

    eta_min = len(new_addresses) * SLEEP_BETWEEN / 60
    print(f"City suffix: {city_suffix!r}")
    print(f"Estimated time: {eta_min:.1f} min at {SLEEP_BETWEEN}s/request\n")

    # ── Fetch new addresses ───────────────────────────────────────────────────
    ok = not_found = errors = 0
    for idx, addr in enumerate(new_addresses, 1):
        result = geocode(addr, city_suffix)
        cache[addr] = result

        status_icon = {"ok": "✓", "not_found": "✗"}.get(result["status"], "!")
        print(f"  [{idx:>4}/{len(new_addresses)}] {status_icon} {addr}  "
              f"→  {result['lat']}, {result['lon']}"
              if result["lat"] else
              f"  [{idx:>4}/{len(new_addresses)}] {status_icon} {addr}  →  {result['status']}")

        if result["status"] == "ok":           ok += 1
        elif result["status"] == "not_found":  not_found += 1
        else:                                  errors += 1

        time.sleep(SLEEP_BETWEEN)

    # ── Write updated cache ───────────────────────────────────────────────────
    fieldnames = ["address", "lat", "lon", "display_name", "status"]
    with open(out_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cache.values())

    print(f"\nWrote {len(cache)} rows → {OUTPUT_CSV}")
    print(f"  Geocoded: {ok}  Not found: {not_found}  Errors: {errors}")
    if not_found or errors:
        print("  Tip: not_found usually means an address format Nominatim doesn't recognise.")
        print("  These will still work in the pipeline — they just won't have coordinates.")


if __name__ == "__main__":
    main()
