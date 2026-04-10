"""
Weehawken MLS Data Cleaning & Appreciation Scoring
-------------------------------------------------
Input:  Testing.csv  (Paragon MLS custom export, Weehawken)
Output: weehawken_cleaned.csv   — cleaned, enriched dataset
        weehawken_scored.csv    — per-property appreciation scores
        weehawken_block.csv     — block-level market summary

Weehawken-specific adaptations vs. jc_heights_clean.py:
  - block_key() falls back to Complex Name when Block is missing (~14% of rows)
  - ZIP normalization: '07030-XXXX' extended zips → '07030'
  - Parking regex: adds Weehawken-specific negatives (optional/rental/off-site)
    because Weehawken listings often mention parking as an available upsell,
    not an included amenity
  - No standalone 'Parking' MLS column — entirely remarks-based detection
"""

import csv
import re
import math
import statistics
from datetime import datetime, date
from collections import defaultdict, Counter
from pathlib import Path

# ── CONFIG ──────────────────────────────────────────────────────────────────
INPUT_FILE   = "Testing.csv"
OUT_CLEAN    = "weehawken_cleaned.csv"
OUT_SCORED   = "weehawken_scored.csv"
OUT_BLOCK    = "weehawken_block.csv"

BLOCKSTREET_MIN  = 5     # min sales on a (block, street) pair to split into sub-key
COMPLEX_MIN      = 5     # min sales on a Complex Name to use it as a block-key fallback
CAGR_WINDOW_START = 2022
CAGR_WINDOW_END   = 2026

SCORE_WEIGHTS = {
    "block_cagr":          0.30,
    "dom_trend":           0.20,
    "sale_to_list_ratio":  0.20,
    "assess_delta":        0.15,
    "inventory_trend":     0.15,
}

# ── HELPERS ──────────────────────────────────────────────────────────────────
def parse_price(s):
    if not s or not s.strip():
        return None
    cleaned = re.sub(r"[^\d.]", "", s.strip())
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except ValueError:
        return None

def parse_date(s):
    if not s or not s.strip():
        return None
    for fmt in ("%m/%d/%Y", "%Y-%m-%d", "%m-%d-%Y"):
        try:
            return datetime.strptime(s.strip(), fmt).date()
        except ValueError:
            continue
    return None

def parse_int(s):
    if not s or not s.strip():
        return None
    try:
        return int(re.sub(r"[^\d]", "", s.strip()))
    except ValueError:
        return None

def parse_float(s):
    if not s or not s.strip():
        return None
    try:
        return float(re.sub(r"[^\d.]", "", s.strip()))
    except ValueError:
        return None

def normalize_zip(z):
    """'07030-4592' → '07030'  |  '07030' → '07030'"""
    if not z:
        return ""
    return z.strip().split("-")[0]

def is_residential(row):
    """Filter out parking spaces, storage units, commercial, etc."""
    beds  = parse_int(row.get("Bedrooms", ""))
    price = parse_price(row.get("Sold Price", "") or row.get("Price", ""))
    unit  = (row.get("Unit Number", "") or "").lower()
    addr  = (row.get("Address",     "") or "").lower()

    non_res_keywords = ["parking", "storage", "garage", "commercial", "land"]
    for kw in non_res_keywords:
        if kw in unit or kw in addr:
            return False

    sqft = parse_int(row.get("Approx Sq Ft", ""))
    if beds == 0 and (sqft is None or sqft < 200):
        return False

    if price is not None and price < 75_000:
        return False

    return True

# These counters are populated in the pre-pass before the main loop
blockstreet_counts: Counter = Counter()
complex_counts: Counter     = Counter()

def norm_block(b):
    """Strip leading zeros from a block number: '00058' → '58', '0' → '0'."""
    try:
        return str(int(b))
    except (ValueError, TypeError):
        return (b or "").lstrip("0") or "0"

def block_key(row):
    """Micro-market key for Weehawken.

    Priority:
      1. Block + Street  (when enough data, same as JC logic)
      2. Block only
      3. Complex Name    (Weehawken-specific: ~14% of rows have no Block)
      4. ZIP             (last resort)

    Block numbers are normalized (leading zeros stripped) so that '00058'
    and '58' map to the same key — Paragon stores them inconsistently.
    """
    block_raw = (row.get("Block", "") or "").strip()
    block     = norm_block(block_raw) if block_raw else ""
    addr  = (row.get("Address", "") or "").strip()
    parts = addr.split()
    street_name = " ".join(parts[1:]) if len(parts) > 1 else ""

    if block:
        if street_name and blockstreet_counts.get((block, street_name), 0) >= BLOCKSTREET_MIN:
            street_key = re.sub(r"[^A-Z0-9]", "", street_name.upper().split()[0])
            return f"block_{block}_{street_key}"
        return f"block_{block}"

    # No Block — fall back to Complex Name when we have enough data
    complex_name = (row.get("Complex Name", "") or "").strip()
    if complex_name and complex_counts.get(complex_name, 0) >= COMPLEX_MIN:
        safe_key = re.sub(r"[^A-Z0-9]", "", complex_name.upper())[:20]
        return f"complex_{safe_key}"

    return f"zip_{normalize_zip(row.get('Zip', ''))}"

def year_quarter(d):
    if d is None:
        return None
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"

# ── STEP 1: LOAD & CLEAN ─────────────────────────────────────────────────────
print("Loading data...")
with open(INPUT_FILE, encoding="utf-8-sig") as f:
    raw = list(csv.DictReader(f))

print(f"  Raw rows: {len(raw)}")

# Pre-pass: populate blockstreet_counts and complex_counts
for _row in raw:
    if not is_residential(_row):
        continue
    _block = (_row.get("Block", "") or "").strip()
    _addr  = (_row.get("Address", "") or "").strip()
    _parts = _addr.split()
    _street = " ".join(_parts[1:]) if len(_parts) > 1 else ""
    if _block and _street:
        blockstreet_counts[(norm_block(_block), _street)] += 1
    if not _block:
        _cx = (_row.get("Complex Name", "") or "").strip()
        if _cx:
            complex_counts[_cx] += 1

print(f"  Block+street pairs with >= {BLOCKSTREET_MIN} sales: "
      f"{sum(1 for v in blockstreet_counts.values() if v >= BLOCKSTREET_MIN)}")
print(f"  Complex Name fallback keys (>= {COMPLEX_MIN} sales): "
      f"{sum(1 for v in complex_counts.values() if v >= COMPLEX_MIN)}")

# ── OPTIONAL ENRICHMENT LOOKUPS ──────────────────────────────────────────────
geocode_lu: dict = {}
geo_path = Path("address_geocoded.csv")
if geo_path.exists():
    with open(geo_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            if _row.get("lat") and _row.get("lon"):
                try:
                    geocode_lu[_row["address"]] = (float(_row["lat"]), float(_row["lon"]))
                except (ValueError, TypeError):
                    pass
    print(f"  Geocode cache: {len(geocode_lu)} addresses with coordinates")
else:
    print("  Geocode cache not found (run geocode.py to generate)")

transit_lu: dict = {}
transit_path = Path("address_transit.csv")
if transit_path.exists():
    with open(transit_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            transit_lu[_row["address"]] = _row
    print(f"  Transit proximity: {len(transit_lu)} addresses loaded")
else:
    print("  Transit data not found (run enrich_transit.py to generate)")

walkscore_lu: dict = {}
ws_path = Path("address_walkscore.csv")
if ws_path.exists():
    with open(ws_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            walkscore_lu[_row["address"]] = _row
    print(f"  Walk Score data: {len(walkscore_lu)} addresses loaded")
else:
    print("  Walk Score data not found (run enrich_walkscore.py to generate)")

modiiv_lot_lu: dict  = {}
modiiv_block_lu: dict = {}
modiiv_lot_path   = Path("modiiv_lot.csv")
modiiv_block_path = Path("modiiv_block.csv")
if modiiv_lot_path.exists() and modiiv_block_path.exists():
    with open(modiiv_lot_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            _b = _row.get("block","").strip()
            _l = _row.get("lot",  "").strip()
            _v = _row.get("net_value","").strip()
            if _b and _l and _v and _v != "0":
                modiiv_lot_lu[(_b, _l)] = int(float(_v))
    with open(modiiv_block_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            _b = _row.get("block","").strip()
            _v = _row.get("median_net_value","").strip()
            if _b and _v and _v != "0":
                modiiv_block_lu[_b] = int(float(_v))
    print(f"  MOD-IV data: {len(modiiv_lot_lu):,} lot pairs, {len(modiiv_block_lu):,} block medians loaded")
else:
    print("  MOD-IV data not found (run enrich_modiiv.py to generate)")

cleaned = []
skipped = 0

for row in raw:
    if not is_residential(row):
        skipped += 1
        continue

    sold_price   = parse_price(row.get("Sold Price", ""))
    list_price   = parse_price(row.get("Asking Price", ""))
    orig_price   = parse_price(row.get("Original List Price", ""))
    closing_date = parse_date(row.get("Closing Date", ""))
    listing_date = parse_date(row.get("Listing Date", ""))
    assessed_val = parse_price(row.get("Assessed Value", ""))
    taxes        = parse_price(row.get("Taxes", ""))
    hoa_fee      = parse_price(row.get("Monthly Maintenance Fee", ""))
    sqft         = parse_int(row.get("Approx Sq Ft", ""))
    beds         = parse_int(row.get("Bedrooms", ""))
    full_baths   = parse_int(row.get("Total # Full Baths", ""))
    half_baths   = parse_int(row.get("Total # Half Baths", ""))
    dom          = parse_int(row.get("Days On Market", ""))
    year_built   = parse_int(row.get("Year Built", ""))
    floor_num_raw = (row.get("Floor Number", "") or "").strip()
    is_short_sale = (row.get("Short Sale (Y/N)", "") or "").strip().upper() == "Y"
    is_bank_owned = (row.get("Bank Owned Y/N", "") or "").strip().upper() == "Y"
    is_distressed = is_short_sale or is_bank_owned

    # ZIP normalization — strip extended zip suffixes
    zip_code = normalize_zip(row.get("Zip", ""))

    # Derived fields
    price_per_sqft   = round(sold_price / sqft, 2) if sold_price and sqft and sqft > 0 else None
    sale_to_list     = round(sold_price / list_price, 4) if sold_price and list_price and list_price > 0 else None
    orig_to_sold_pct = round((sold_price - orig_price) / orig_price * 100, 2) if sold_price and orig_price and orig_price > 0 else None
    total_baths      = (full_baths or 0) + 0.5 * (half_baths or 0)
    closing_year     = closing_date.year if closing_date else None
    closing_quarter  = year_quarter(closing_date)
    block            = block_key(row)

    floor_map = {"GRD": 0, "G": 0, "B": -1, "BL": -1, "L": 1}
    try:
        floor_num = int(floor_num_raw)
    except (ValueError, TypeError):
        floor_num = floor_map.get(floor_num_raw.upper(), None)

    # ── Enrichment: Walk Score ────────────────────────────────────────────
    _ws = walkscore_lu.get(row.get("Address", "").strip(), {})
    walk_score    = parse_int(_ws.get("walk_score",    "")) if _ws else None
    transit_score = parse_int(_ws.get("transit_score", "")) if _ws else None
    bike_score    = parse_int(_ws.get("bike_score",    "")) if _ws else None

    # ── Enrichment: Transit proximity ────────────────────────────────────
    _tr = transit_lu.get(row.get("Address", "").strip(), {})
    path_station    = _tr.get("path_station",    "") or None
    path_dist_mi    = parse_float(_tr.get("path_dist_mi",    ""))
    hblr_station    = _tr.get("hblr_station",    "") or None
    hblr_dist_mi    = parse_float(_tr.get("hblr_dist_mi",    ""))
    transit_station = _tr.get("transit_station", "") or None
    transit_dist_mi = parse_float(_tr.get("transit_dist_mi", ""))

    # ── Enrichment: MOD-IV assessed value ────────────────────────────────
    _block_raw  = row.get("Block", "").strip()
    _lot_raw    = row.get("Lot",   "").strip()
    _block_norm = str(int(_block_raw)) if _block_raw.isdigit() else _block_raw.lstrip("0") or "0"
    _lot_norm   = str(int(_lot_raw))   if _lot_raw.isdigit()   else _lot_raw.lstrip("0")   or "0"
    _modiiv_val = (
        modiiv_lot_lu.get((_block_norm, _lot_norm))
        or modiiv_lot_lu.get((_lot_norm, _block_norm))
        or modiiv_block_lu.get(_block_norm)
        or modiiv_block_lu.get(_lot_norm)
    )
    modiiv_assessed    = _modiiv_val if _modiiv_val and _modiiv_val > 0 else None
    effective_assessed = assessed_val or modiiv_assessed
    assess_ratio = round(sold_price / effective_assessed, 4) if sold_price and effective_assessed and effective_assessed > 0 else None

    # ── Parse parking and outdoor from Advertising Remarks ───────────────
    # Weehawken-specific parking logic:
    #   Many Weehawken listings mention parking as an available *upsell* rather
    #   than an included amenity.  "optional parking", "rental parking available",
    #   and "off-site parking" are explicitly excluded from the positive signal.
    remarks = (row.get("Advertising Remarks", "") or "").strip()
    _rem = remarks.lower()

    _park_strong = bool(re.search(
        r'\b(deeded\s+parking|indoor\s+parking|garage\s+parking|assigned\s+parking|'
        r'private\s+parking|parking\s+space|parking\s+spot|parking\s+included|'
        r'1\s+car\s+garage|2\s+car\s+garage|parking\s+deeded|deeded\s+garage)\b', _rem))

    # Weehawken-specific negatives: street permit parking, not deeded/included
    _park_neg = bool(re.search(
        r'\b(no\s+parking|street\s+parking\s+only|street\s+parking\s+permits?|'
        r'permit\s+parking\s+only|optional\s+parking|rental\s+parking|'
        r'off-?site\s+parking)\b', _rem))

    _park_weak = bool(re.search(r'\b(parking|garage|carport|driveway)\b', _rem))

    if _park_neg:
        parking, parking_conf = False, "high"
    elif _park_strong:
        parking, parking_conf = True, "high"
    elif _park_weak:
        parking, parking_conf = True, "medium"
    else:
        parking, parking_conf = False, "low"

    _outdoor_types = {
        "rooftop": r'\b(rooftop|roof\s+top|roof\s+deck)\b',
        "terrace": r'\bterrace\b',
        "balcony": r'\bbalcon(y|ies)\b',
        "deck":    r'\bdeck\b',
        "yard":    r'\b(backyard|back\s+yard|front\s+yard|private\s+yard|yard)\b',
        "patio":   r'\bpatio\b',
    }
    outdoor_type = next(
        (otype for otype, pat in _outdoor_types.items() if re.search(pat, _rem)),
        None
    )
    outdoor      = outdoor_type is not None
    outdoor_conf = "high" if outdoor else "low"

    c = {
        "mls_id":           row.get("MLS #", "").strip(),
        "address":          row.get("Address", "").strip(),
        "unit_number":      row.get("Unit Number", "").strip(),
        "full_address":     f"{row.get('Address','').strip()} {row.get('Unit Number','').strip()}".strip(),
        "zip":              zip_code,
        "area":             row.get("Area", "").strip(),
        "block":            block,
        "block_raw":        row.get("Block", "").strip(),
        "lot":              row.get("Lot", "").strip(),
        "complex_name":     row.get("Complex Name", "").strip(),
        "property_type":    row.get("Type", "").strip(),
        "class":            row.get("Class", "").strip(),
        "status":           row.get("Status", "").strip(),
        "year_built":       year_built,
        "floor_number":     floor_num,
        "bedrooms":         beds,
        "full_baths":       full_baths,
        "half_baths":       half_baths,
        "total_baths":      total_baths,
        "sqft":             sqft,
        "sold_price":       sold_price,
        "list_price":       list_price,
        "orig_list_price":  orig_price,
        "price_per_sqft":   price_per_sqft,
        "sale_to_list":     sale_to_list,
        "orig_to_sold_pct": orig_to_sold_pct,
        "taxes":            taxes,
        "hoa_monthly":      hoa_fee,
        "dom":              dom,
        "closing_date":     closing_date.isoformat() if closing_date else None,
        "listing_date":     listing_date.isoformat() if listing_date else None,
        "closing_year":     closing_year,
        "closing_quarter":  closing_quarter,
        "is_distressed":    is_distressed,
        "is_short_sale":    is_short_sale,
        "is_bank_owned":    is_bank_owned,
        "lat":              geocode_lu.get(row.get("Address","").strip(), (parse_float(row.get("Y Coordinates","")), None))[0],
        "lon":              geocode_lu.get(row.get("Address","").strip(), (None, parse_float(row.get("X Coordinates",""))))[1],
        "between":          row.get("Between", "").strip(),
        "remarks":          remarks,
        "parking":          parking,
        "parking_conf":     parking_conf,
        "outdoor":          outdoor,
        "outdoor_type":     outdoor_type,
        "outdoor_conf":     outdoor_conf,
        "walk_score":       walk_score,
        "transit_score":    transit_score,
        "bike_score":       bike_score,
        "path_station":     path_station,
        "path_dist_mi":     path_dist_mi,
        "hblr_station":     hblr_station,
        "hblr_dist_mi":     hblr_dist_mi,
        "transit_station":  transit_station,
        "transit_dist_mi":  transit_dist_mi,
        "modiiv_assessed":  modiiv_assessed,
        "assessed_value":   effective_assessed,
        "assess_ratio":     assess_ratio,
    }
    cleaned.append(c)

print(f"  After filtering: {len(cleaned)} rows (skipped {skipped})")

# ── STEP 2: BLOCK-LEVEL MARKET ANALYSIS ─────────────────────────────────────
print("\nComputing block-level market signals...")

sold = [r for r in cleaned if r["sold_price"] and r["closing_date"] and not r["is_distressed"]]

block_sales      = defaultdict(list)
block_year_sales = defaultdict(lambda: defaultdict(list))

for r in sold:
    block_sales[r["block"]].append(r)
    if r["closing_year"]:
        block_year_sales[r["block"]][r["closing_year"]].append(r)

block_stats = {}
MIN_SALES = 5

for block, sales in block_sales.items():
    years = sorted(block_year_sales[block].keys())

    if len(years) < 2 or len(sales) < MIN_SALES:
        block_stats[block] = None
        continue

    year_medians = {}
    for yr in years:
        yr_sales = block_year_sales[block][yr]
        psf = [s["price_per_sqft"] for s in yr_sales if s["price_per_sqft"]]
        if psf:
            year_medians[yr] = statistics.median(psf)

    if len(year_medians) < 2:
        block_stats[block] = None
        continue

    yr_list = sorted(year_medians.keys())
    window_medians = {yr: med for yr, med in year_medians.items()
                      if CAGR_WINDOW_START <= yr <= CAGR_WINDOW_END}
    if len(window_medians) >= 2:
        start_yr  = min(window_medians.keys())
        end_yr    = max(window_medians.keys())
        start_med = window_medians[start_yr]
        end_med   = window_medians[end_yr]
    else:
        start_yr  = yr_list[0]
        end_yr    = yr_list[-1]
        start_med = year_medians[start_yr]
        end_med   = year_medians[end_yr]
    n_years = end_yr - start_yr

    if n_years > 0 and start_med > 0:
        cagr = (end_med / start_med) ** (1 / n_years) - 1
    else:
        cagr = None

    year_dom = {}
    for yr in years:
        yr_sales = block_year_sales[block][yr]
        doms = [s["dom"] for s in yr_sales if s["dom"] is not None]
        if doms:
            year_dom[yr] = statistics.median(doms)

    dom_trend = None
    if len(year_dom) >= 2:
        dom_yrs   = sorted(year_dom.keys())
        early_dom = statistics.mean([year_dom[y] for y in dom_yrs[:len(dom_yrs)//2]])
        late_dom  = statistics.mean([year_dom[y] for y in dom_yrs[len(dom_yrs)//2:]])
        dom_trend = math.log(max(late_dom, 1.0) / max(early_dom, 1.0))

    stl_ratios = [s["sale_to_list"] for s in sales if s["sale_to_list"] is not None]
    median_stl = statistics.median(stl_ratios) if stl_ratios else None

    assess_ratios = [s["assess_ratio"] for s in sales if s["assess_ratio"] is not None]
    median_assess_ratio = statistics.median(assess_ratios) if assess_ratios else None

    early_years = yr_list[:len(yr_list)//2]
    late_years  = yr_list[len(yr_list)//2:]
    early_vol   = sum(len(block_year_sales[block][y]) for y in early_years) / max(len(early_years), 1)
    late_vol    = sum(len(block_year_sales[block][y]) for y in late_years)  / max(len(late_years),  1)
    vol_trend   = math.log(max(late_vol, 0.5) / max(early_vol, 0.5))

    block_stats[block] = {
        "block":                block,
        "n_sales":              len(sales),
        "years_covered":        f"{start_yr}-{end_yr}",
        "cagr_price_per_sqft":  round(cagr * 100, 2) if cagr is not None else None,
        "median_price_per_sqft":round(end_med, 0),
        "median_dom":           round(statistics.median([s["dom"] for s in sales if s["dom"] is not None]), 1) if any(s["dom"] for s in sales) else None,
        "dom_trend":            round(dom_trend, 4) if dom_trend is not None else None,
        "median_sale_to_list":  round(median_stl, 4) if median_stl is not None else None,
        "median_assess_ratio":  round(median_assess_ratio, 4) if median_assess_ratio is not None else None,
        "vol_trend":            round(vol_trend, 4),
    }

valid_blocks = {k: v for k, v in block_stats.items() if v is not None}
print(f"  Blocks with enough data: {len(valid_blocks)} / {len(block_stats)}")

# ── STEP 3: NORMALIZE & SCORE ────────────────────────────────────────────────
print("\nNormalizing and scoring...")

def normalize(values, invert=False):
    import bisect
    non_null = sorted([v for v in values if v is not None])
    n = len(non_null)
    if n == 0:
        return {i: None for i in range(len(values))}
    result = {}
    for i, v in enumerate(values):
        if v is None:
            result[i] = None
        elif n == 1:
            result[i] = 0.5
        else:
            lo   = bisect.bisect_left(non_null, v)
            hi   = bisect.bisect_right(non_null, v)
            rank = (lo + hi) / 2 / n
            result[i] = (1.0 - rank) if invert else rank
    return result

blocks_list = list(valid_blocks.values())

cagr_norm   = normalize([b["cagr_price_per_sqft"] for b in blocks_list])
dom_norm    = normalize([b["dom_trend"]            for b in blocks_list], invert=True)
stl_norm    = normalize([b["median_sale_to_list"]  for b in blocks_list])
assess_norm = normalize([b["median_assess_ratio"]  for b in blocks_list])
vol_norm    = normalize([b["vol_trend"]            for b in blocks_list], invert=True)

for i, b in enumerate(blocks_list):
    scores = {
        "block_cagr":         cagr_norm[i],
        "dom_trend":          dom_norm[i],
        "sale_to_list_ratio": stl_norm[i],
        "assess_delta":       assess_norm[i],
        "inventory_trend":    vol_norm[i],
    }
    weighted = sum(SCORE_WEIGHTS[k] * v for k, v in scores.items() if v is not None)
    total_weight = sum(SCORE_WEIGHTS[k] for k, v in scores.items() if v is not None)
    b["appreciation_score"] = round(weighted / total_weight * 100, 1) if total_weight > 0 else None
    b.update({f"score_{k}": round(v * 100, 1) if v is not None else None for k, v in scores.items()})

ranked = sorted([b for b in blocks_list if b["appreciation_score"] is not None],
                key=lambda x: x["appreciation_score"], reverse=True)
for i, b in enumerate(ranked):
    b["block_rank"] = i + 1

# ── STEP 4: JOIN SCORES BACK TO PROPERTIES ──────────────────────────────────
print("Joining scores to properties...")

block_score_lookup = {b["block"]: b for b in blocks_list}

scored = []
for r in cleaned:
    row = dict(r)
    bs = block_score_lookup.get(r["block"])
    if bs:
        row["block_appreciation_score"] = bs.get("appreciation_score")
        row["block_rank"]               = bs.get("block_rank")
        row["block_cagr_pct"]           = bs.get("cagr_price_per_sqft")
        row["block_median_psf"]         = bs.get("median_price_per_sqft")
        row["block_median_dom"]         = bs.get("median_dom")
        row["block_median_stl"]         = bs.get("median_sale_to_list")
        row["block_n_sales"]            = bs.get("n_sales")
    else:
        row["block_appreciation_score"] = None
        row["block_rank"]               = None
        row["block_cagr_pct"]           = None
        row["block_median_psf"]         = None
        row["block_median_dom"]         = None
        row["block_median_stl"]         = None
        row["block_n_sales"]            = None
    scored.append(row)

# ── STEP 5: WRITE OUTPUT FILES ───────────────────────────────────────────────
print("\nWriting output files...")

def write_csv(data, path):
    if not data:
        print(f"  WARNING: no data to write to {path}")
        return
    with open(path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=data[0].keys())
        writer.writeheader()
        writer.writerows(data)
    print(f"  Wrote {len(data)} rows -> {path}")

write_csv(cleaned, OUT_CLEAN)
write_csv(scored,  OUT_SCORED)
write_csv(ranked,  OUT_BLOCK)

# ── STEP 6: SUMMARY REPORT ───────────────────────────────────────────────────
print("\n" + "="*60)
print("SUMMARY")
print("="*60)
print(f"Total properties cleaned:     {len(cleaned)}")
print(f"Sold (arm's-length):          {len(sold)}")
print(f"Blocks scored:                {len(ranked)}")

# Block key type breakdown
key_types = Counter(b["block"].split("_")[0] for b in cleaned)
print(f"Block key types:              {dict(key_types)}")

# Parking signal breakdown
park_high  = sum(1 for r in cleaned if r["parking"] and r["parking_conf"] == "high")
park_med   = sum(1 for r in cleaned if r["parking"] and r["parking_conf"] == "medium")
park_none  = sum(1 for r in cleaned if not r["parking"])
print(f"Parking — high conf: {park_high}  medium: {park_med}  none/negative: {park_none}")

print()
print("TOP 10 BLOCKS BY APPRECIATION SCORE:")
print(f"{'Rank':<5} {'Block':<25} {'Score':<8} {'CAGR%':<8} {'Med PSF':<10} {'Med DOM':<10} {'STL':<6} {'Sales'}")
print("-"*80)
for b in ranked[:10]:
    print(f"{b['block_rank']:<5} {b['block']:<25} {b['appreciation_score']:<8} "
          f"{str(b['cagr_price_per_sqft'])+'%':<8} "
          f"${b['median_price_per_sqft']:<9,.0f} "
          f"{str(b['median_dom']):<10} "
          f"{b['median_sale_to_list']:<6} "
          f"{b['n_sales']}")

print("\nDone.")
