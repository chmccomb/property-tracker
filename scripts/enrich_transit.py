"""
Transit Proximity Enrichment
------------------------------
Computes distance from each property to the nearest PATH station and nearest
Hudson-Bergen Light Rail (HBLR) station. Reads coordinates from the geocode
cache produced by geocode.py.

No API or key required — station coordinates are hardcoded.

Output:
    address_transit.csv — joined into the pipeline by jc_heights_clean.py

Run from data/processed/:
    python ../../scripts/enrich_transit.py
"""

import csv
import math
from pathlib import Path

GEOCODE_CSV = "address_geocoded.csv"
OUTPUT_CSV  = "address_transit.csv"

# ── Station coordinates ───────────────────────────────────────────────────────
# Heights residents primarily use Journal Square PATH + HBLR Tonnelle Ave.
# All distances computed as straight-line (haversine); walking distances are
# roughly 20–25% longer depending on grid.

PATH_STATIONS = [
    ("Journal Square",  40.7327, -74.0632),
    ("Grove Street",    40.7191, -74.0431),
    ("Exchange Place",  40.7165, -74.0323),
    ("Newport",         40.7271, -74.0336),
    ("Hoboken",         40.7358, -74.0248),
    ("Harrison",        40.7459, -74.1541),
    ("Newark Penn",     40.7346, -74.1641),
]

HBLR_STATIONS = [
    # Main line — north end (most relevant to Heights)
    ("Tonnelle Ave",        40.7521, -74.0659),
    ("West 49th St",        40.7656, -74.0538),  # North Bergen (close to Heights border)
    # Main line — south/central
    ("Pavonia/Newport",     40.7277, -74.0337),
    ("Harsimus Cove",       40.7228, -74.0383),
    ("Essex Street",        40.7166, -74.0424),
    ("Jersey Ave",          40.7143, -74.0496),
    ("MLK Drive",           40.7023, -74.0735),
    ("West Side Ave",       40.7188, -74.0819),
    ("Danforth Ave",        40.7122, -74.0771),
    ("Garfield Ave",        40.7056, -74.0791),
    ("Liberty State Park",  40.7027, -74.0612),
    ("Communipaw Ave",      40.7090, -74.0684),
    ("West Bergen",         40.6978, -74.0900),
    # Waterfront branch
    ("Port Imperial",       40.7509, -74.0240),
    ("Lincoln Harbor",      40.7264, -74.0250),
]


def haversine_miles(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Straight-line distance in miles between two lat/lon points."""
    R = 3958.8  # Earth radius in miles
    d_lat = math.radians(lat2 - lat1)
    d_lon = math.radians(lon2 - lon1)
    a = (math.sin(d_lat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(d_lon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def nearest(lat: float, lon: float, stations: list) -> tuple:
    """Return (station_name, distance_miles) for the closest station."""
    best_name, best_dist = None, float("inf")
    for name, slat, slon in stations:
        d = haversine_miles(lat, lon, slat, slon)
        if d < best_dist:
            best_dist = d
            best_name = name
    return best_name, round(best_dist, 3)


def main() -> None:
    geo_path = Path(GEOCODE_CSV)
    if not geo_path.exists():
        print(f"ERROR: {GEOCODE_CSV} not found. Run geocode.py first.")
        return

    with open(geo_path, encoding="utf-8") as f:
        geocoded = [r for r in csv.DictReader(f) if r.get("lat") and r.get("lon")]

    print(f"Computing transit proximity for {len(geocoded)} geocoded addresses …")

    results = []
    for row in geocoded:
        try:
            lat = float(row["lat"])
            lon = float(row["lon"])
        except (ValueError, TypeError):
            continue

        path_name, path_dist   = nearest(lat, lon, PATH_STATIONS)
        hblr_name, hblr_dist   = nearest(lat, lon, HBLR_STATIONS)
        # Combined: nearest station of either type
        transit_name = path_name if path_dist <= hblr_dist else hblr_name
        transit_dist = min(path_dist, hblr_dist)

        results.append({
            "address":          row["address"],
            "path_station":     path_name,
            "path_dist_mi":     path_dist,
            "hblr_station":     hblr_name,
            "hblr_dist_mi":     hblr_dist,
            "transit_station":  transit_name,
            "transit_dist_mi":  transit_dist,
        })

    fieldnames = [
        "address", "path_station", "path_dist_mi",
        "hblr_station", "hblr_dist_mi", "transit_station", "transit_dist_mi",
    ]
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(results)

    print(f"Wrote {len(results)} rows → {OUTPUT_CSV}")
    print()

    # Summary: distribution of nearest PATH distances
    path_dists = [r["path_dist_mi"] for r in results]
    buckets = [(0.25, "< 0.25 mi"), (0.50, "0.25–0.50 mi"),
               (0.75, "0.50–0.75 mi"), (1.00, "0.75–1.0 mi")]
    print("PATH distance distribution:")
    prev = 0
    for cutoff, label in buckets:
        n = sum(1 for d in path_dists if prev <= d < cutoff)
        print(f"  {label}: {n} addresses")
        prev = cutoff
    print(f"  > 1.0 mi: {sum(1 for d in path_dists if d >= 1.0)} addresses")


if __name__ == "__main__":
    main()
