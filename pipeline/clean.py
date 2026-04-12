"""
Unified MLS Data Cleaning & Appreciation Scoring
-------------------------------------------------
Config-driven replacement for jc_heights_clean.py, hoboken_clean.py, and
weehawken_clean.py.  All city-specific behaviour is controlled by the config
dict passed to run_clean().

Usage:
    python -m pipeline.clean --city jc_heights
    python -m pipeline.clean --city hoboken --data-dir data/processed/hoboken
    python -m pipeline.clean --city all --csv path/to/combined_export.csv
"""

import argparse
import bisect
import csv
import math
import re
import statistics
import sys
from collections import Counter, defaultdict
from datetime import date, datetime
from pathlib import Path

from pipeline.config import CITY_COLUMN_MAP, CITY_CONFIGS

# ── PARSE HELPERS (shared across all cities) ────────────────────────────────

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
    if not z:
        return ""
    return z.strip().split("-")[0]

def norm_block(b):
    try:
        return str(int(b))
    except (ValueError, TypeError):
        return (b or "").lstrip("0") or "0"

def is_residential(row):
    beds = parse_int(row.get("Bedrooms", ""))
    price = parse_price(row.get("Sold Price", "") or row.get("Price", ""))
    unit = (row.get("Unit Number", "") or "").lower()
    addr = (row.get("Address", "") or "").lower()

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

def year_quarter(d):
    if d is None:
        return None
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"


# ── BLOCK KEY (config-driven) ──────────────────────────────────────────────

def make_block_key_fn(cfg, blockstreet_counts, complex_counts):
    """Return a block_key(row) function parameterized by city config."""

    use_complex = cfg["complex_fallback"]
    complex_min = cfg.get("complex_min", 5)
    blockstreet_min = cfg["blockstreet_min"]
    do_zip_norm = cfg["zip_normalize"]
    do_norm_block = cfg["norm_block_leading_zeros"]

    def block_key(row):
        block_raw = (row.get("Block", "") or "").strip()
        block = norm_block(block_raw) if (block_raw and do_norm_block) else block_raw
        if not block:
            block = ""

        addr = (row.get("Address", "") or "").strip()
        parts = addr.split()
        street_name = " ".join(parts[1:]) if len(parts) > 1 else ""

        if block:
            key = (block, street_name) if do_norm_block else (block_raw, street_name)
            if street_name and blockstreet_counts.get((norm_block(block_raw) if do_norm_block else block_raw, street_name), 0) >= blockstreet_min:
                street_key = re.sub(r"[^A-Z0-9]", "", street_name.upper().split()[0])
                return f"block_{block}_{street_key}"
            return f"block_{block}"

        if use_complex:
            complex_name = (row.get("Complex Name", "") or "").strip()
            if complex_name and complex_counts.get(complex_name, 0) >= complex_min:
                safe_key = re.sub(r"[^A-Z0-9]", "", complex_name.upper())[:20]
                return f"complex_{safe_key}"

        zip_val = row.get("Zip", "").strip()
        if do_zip_norm:
            zip_val = normalize_zip(zip_val)
        return f"zip_{zip_val}"

    return block_key


# ── PARKING DETECTION (config-driven) ──────────────────────────────────────

def detect_parking(remarks_lower, cfg):
    strong_base = (
        r'\b(deeded\s+parking|indoor\s+parking|garage\s+parking|assigned\s+parking|'
        r'private\s+parking|parking\s+space|parking\s+spot|parking\s+included|'
        r'1\s+car\s+garage|2\s+car\s+garage|parking\s+deeded'
    )
    extra = cfg.get("parking_strong_extra", "")
    strong_pattern = strong_base + extra + r')\b'

    _park_strong = bool(re.search(strong_pattern, remarks_lower))
    _park_weak = bool(re.search(r'\b(parking|garage|carport|driveway)\b', remarks_lower))
    _park_neg = bool(re.search(cfg["parking_neg_pattern"], remarks_lower))

    if _park_neg:
        return False, "high"
    elif _park_strong:
        return True, "high"
    elif _park_weak:
        return True, "medium"
    else:
        return False, "low"


def detect_outdoor(remarks_lower):
    _outdoor_types = {
        "rooftop": r'\b(rooftop|roof\s+top|roof\s+deck)\b',
        "terrace": r'\bterrace\b',
        "balcony": r'\bbalcon(y|ies)\b',
        "deck":    r'\bdeck\b',
        "yard":    r'\b(backyard|back\s+yard|front\s+yard|private\s+yard|yard)\b',
        "patio":   r'\bpatio\b',
    }
    outdoor_type = next(
        (otype for otype, pat in _outdoor_types.items() if re.search(pat, remarks_lower)),
        None
    )
    outdoor = outdoor_type is not None
    outdoor_conf = "high" if outdoor else "low"
    return outdoor, outdoor_type, outdoor_conf


# ── ENRICHMENT LOADERS ─────────────────────────────────────────────────────

def load_enrichments(data_dir):
    """Load all optional enrichment CSV caches from data_dir. Returns dict of lookups."""
    data_dir = Path(data_dir)

    geocode_lu = {}
    geo_path = data_dir / "address_geocoded.csv"
    if geo_path.exists():
        with open(geo_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if row.get("lat") and row.get("lon"):
                    try:
                        geocode_lu[row["address"]] = (float(row["lat"]), float(row["lon"]))
                    except (ValueError, TypeError):
                        pass
        print(f"  Geocode cache: {len(geocode_lu)} addresses with coordinates")
    else:
        print("  Geocode cache not found (run geocode.py to generate)")

    transit_lu = {}
    transit_path = data_dir / "address_transit.csv"
    if transit_path.exists():
        with open(transit_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                transit_lu[row["address"]] = row
        print(f"  Transit proximity: {len(transit_lu)} addresses loaded")
    else:
        print("  Transit data not found (run enrich_transit.py to generate)")

    walkscore_lu = {}
    ws_path = data_dir / "address_walkscore.csv"
    if ws_path.exists():
        with open(ws_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                walkscore_lu[row["address"]] = row
        print(f"  Walk Score data: {len(walkscore_lu)} addresses loaded")
    else:
        print("  Walk Score data not found (run enrich_walkscore.py to generate)")

    modiiv_lot_lu = {}
    modiiv_block_lu = {}
    modiiv_lot_path = data_dir / "modiiv_lot.csv"
    modiiv_block_path = data_dir / "modiiv_block.csv"
    if modiiv_lot_path.exists() and modiiv_block_path.exists():
        with open(modiiv_lot_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                _b = row.get("block", "").strip()
                _l = row.get("lot", "").strip()
                _v = row.get("net_value", "").strip()
                if _b and _l and _v and _v != "0":
                    modiiv_lot_lu[(_b, _l)] = int(float(_v))
        with open(modiiv_block_path, encoding="utf-8") as f:
            for row in csv.DictReader(f):
                _b = row.get("block", "").strip()
                _v = row.get("median_net_value", "").strip()
                if _b and _v and _v != "0":
                    modiiv_block_lu[_b] = int(float(_v))
        print(f"  MOD-IV data: {len(modiiv_lot_lu):,} lot pairs, {len(modiiv_block_lu):,} block medians loaded")
    else:
        print("  MOD-IV data not found (run enrich_modiiv.py to generate)")

    return {
        "geocode": geocode_lu,
        "transit": transit_lu,
        "walkscore": walkscore_lu,
        "modiiv_lot": modiiv_lot_lu,
        "modiiv_block": modiiv_block_lu,
    }


# ── NORMALIZE & SCORE ──────────────────────────────────────────────────────

def percentile_rank_normalize(values, invert=False):
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
            lo = bisect.bisect_left(non_null, v)
            hi = bisect.bisect_right(non_null, v)
            rank = (lo + hi) / 2 / n
            result[i] = (1.0 - rank) if invert else rank
    return result


# ── MAIN PIPELINE ──────────────────────────────────────────────────────────

def run_clean(city_key, data_dir=None, csv_path=None, rows=None):
    """
    Run the cleaning pipeline for a single city.

    Args:
        city_key: Key in CITY_CONFIGS (e.g. 'jc_heights', 'hoboken')
        data_dir: Directory containing enrichment files and where output is written
        csv_path: Path to input CSV (overrides default data_dir/Testing.csv)
        rows: Pre-loaded list of CSV row dicts (overrides csv_path). Used when
              processing a combined export that's been pre-filtered by city.

    Returns:
        dict with keys: cleaned, scored, ranked, block_stats
    """
    cfg = CITY_CONFIGS[city_key]
    prefix = cfg["output_prefix"]

    if data_dir is None:
        data_dir = Path(".")
    else:
        data_dir = Path(data_dir)

    print(f"\n{'='*60}")
    print(f"  Cleaning: {cfg['name']} ({city_key})")
    print(f"  Data dir: {data_dir}")
    print(f"{'='*60}")

    # ── Load data ────────────────────────────────────────────────────────
    if rows is not None:
        raw = rows
        print(f"  Pre-filtered rows: {len(raw)}")
    else:
        input_path = csv_path or (data_dir / "Testing.csv")
        print(f"Loading data from {input_path}...")
        with open(input_path, encoding="utf-8-sig") as f:
            raw = list(csv.DictReader(f))
        print(f"  Raw rows: {len(raw)}")

    # ── Load enrichments ─────────────────────────────────────────────────
    enrich = load_enrichments(data_dir)
    geocode_lu = enrich["geocode"]
    transit_lu = enrich["transit"]
    walkscore_lu = enrich["walkscore"]
    modiiv_lot_lu = enrich["modiiv_lot"]
    modiiv_block_lu = enrich["modiiv_block"]

    # ── Pre-pass: count block+street and complex occurrences ─────────────
    blockstreet_counts = Counter()
    complex_counts = Counter()
    do_norm = cfg["norm_block_leading_zeros"]

    for _row in raw:
        if not is_residential(_row):
            continue
        _block = (_row.get("Block", "") or "").strip()
        _addr = (_row.get("Address", "") or "").strip()
        _parts = _addr.split()
        _street = " ".join(_parts[1:]) if len(_parts) > 1 else ""
        if _block and _street:
            bk = norm_block(_block) if do_norm else _block
            blockstreet_counts[(bk, _street)] += 1
        if not _block and cfg["complex_fallback"]:
            _cx = (_row.get("Complex Name", "") or "").strip()
            if _cx:
                complex_counts[_cx] += 1

    print(f"  Block+street pairs with >= {cfg['blockstreet_min']} sales: "
          f"{sum(1 for v in blockstreet_counts.values() if v >= cfg['blockstreet_min'])}")
    if cfg["complex_fallback"]:
        print(f"  Complex Name fallback keys (>= {cfg['complex_min']} sales): "
              f"{sum(1 for v in complex_counts.values() if v >= cfg['complex_min'])}")

    block_key = make_block_key_fn(cfg, blockstreet_counts, complex_counts)

    # ── Main cleaning loop ───────────────────────────────────────────────
    cleaned = []
    skipped = 0

    for row in raw:
        if not is_residential(row):
            skipped += 1
            continue

        sold_price    = parse_price(row.get("Sold Price", ""))
        list_price    = parse_price(row.get("Asking Price", ""))
        orig_price    = parse_price(row.get("Original List Price", ""))
        closing_date  = parse_date(row.get("Closing Date", ""))
        listing_date  = parse_date(row.get("Listing Date", ""))
        assessed_val  = parse_price(row.get("Assessed Value", ""))
        taxes         = parse_price(row.get("Taxes", ""))
        hoa_fee       = parse_price(row.get("Monthly Maintenance Fee", ""))
        sqft          = parse_int(row.get("Approx Sq Ft", ""))
        beds          = parse_int(row.get("Bedrooms", ""))
        full_baths    = parse_int(row.get("Total # Full Baths", ""))
        half_baths    = parse_int(row.get("Total # Half Baths", ""))
        dom           = parse_int(row.get("Days On Market", ""))
        year_built    = parse_int(row.get("Year Built", ""))
        floor_num_raw = (row.get("Floor Number", "") or "").strip()
        is_short_sale = (row.get("Short Sale (Y/N)", "") or "").strip().upper() == "Y"
        is_bank_owned = (row.get("Bank Owned Y/N", "") or "").strip().upper() == "Y"
        is_distressed = is_short_sale or is_bank_owned

        # ZIP normalization
        zip_raw = row.get("Zip", "").strip()
        zip_code = normalize_zip(zip_raw) if cfg["zip_normalize"] else zip_raw

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
            floor_num = floor_map.get(floor_num_raw.upper(), None) if floor_num_raw else None

        # Walk Score
        _ws = walkscore_lu.get(row.get("Address", "").strip(), {})
        walk_score    = parse_int(_ws.get("walk_score", "")) if _ws else None
        transit_score = parse_int(_ws.get("transit_score", "")) if _ws else None
        bike_score    = parse_int(_ws.get("bike_score", "")) if _ws else None

        # Transit proximity
        _tr = transit_lu.get(row.get("Address", "").strip(), {})
        path_station    = _tr.get("path_station", "") or None
        path_dist_mi    = parse_float(_tr.get("path_dist_mi", ""))
        hblr_station    = _tr.get("hblr_station", "") or None
        hblr_dist_mi    = parse_float(_tr.get("hblr_dist_mi", ""))
        transit_station = _tr.get("transit_station", "") or None
        transit_dist_mi = parse_float(_tr.get("transit_dist_mi", ""))

        # MOD-IV assessed value
        _block_raw  = row.get("Block", "").strip()
        _lot_raw    = row.get("Lot", "").strip()
        _block_norm = str(int(_block_raw)) if _block_raw.isdigit() else _block_raw.lstrip("0") or "0"
        _lot_norm   = str(int(_lot_raw)) if _lot_raw.isdigit() else _lot_raw.lstrip("0") or "0"
        _modiiv_val = (
            modiiv_lot_lu.get((_block_norm, _lot_norm))
            or modiiv_lot_lu.get((_lot_norm, _block_norm))
            or modiiv_block_lu.get(_block_norm)
            or modiiv_block_lu.get(_lot_norm)
        )
        modiiv_assessed    = _modiiv_val if _modiiv_val and _modiiv_val > 0 else None
        effective_assessed = assessed_val or modiiv_assessed
        assess_ratio = round(sold_price / effective_assessed, 4) if sold_price and effective_assessed and effective_assessed > 0 else None

        # Parking and outdoor
        remarks = (row.get("Advertising Remarks", "") or "").strip()
        _rem = remarks.lower()
        parking, parking_conf = detect_parking(_rem, cfg)
        outdoor, outdoor_type, outdoor_conf = detect_outdoor(_rem)

        # Geocode: prefer cache, fall back to MLS Y/X coordinates
        addr_key = row.get("Address", "").strip()
        geo_coords = geocode_lu.get(addr_key)
        if geo_coords:
            lat, lon = geo_coords
        else:
            lat = parse_float(row.get("Y Coordinates", ""))
            lon = parse_float(row.get("X Coordinates", ""))

        c = {
            "mls_id":           row.get("MLS #", "").strip(),
            "address":          addr_key,
            "unit_number":      row.get("Unit Number", "").strip(),
            "full_address":     f"{addr_key} {row.get('Unit Number', '').strip()}".strip(),
            "zip":              zip_code,
            "area":             row.get("Area", "").strip(),
            "block":            block,
            "block_raw":        _block_raw,
            "lot":              _lot_raw,
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
            "lat":              lat,
            "lon":              lon,
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

    # ── BLOCK-LEVEL MARKET ANALYSIS ──────────────────────────────────────
    print("\nComputing block-level market signals...")
    SCORE_WEIGHTS = cfg["score_weights"]
    CAGR_WINDOW_START = cfg["cagr_window_start"]
    CAGR_WINDOW_END = cfg["cagr_window_end"]
    MIN_SALES = 5

    sold = [r for r in cleaned if r["sold_price"] and r["closing_date"] and not r["is_distressed"]]

    block_sales = defaultdict(list)
    block_year_sales = defaultdict(lambda: defaultdict(list))
    for r in sold:
        block_sales[r["block"]].append(r)
        if r["closing_year"]:
            block_year_sales[r["block"]][r["closing_year"]].append(r)

    block_stats = {}
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
        early_vol = sum(len(block_year_sales[block][y]) for y in early_years) / max(len(early_years), 1)
        late_vol  = sum(len(block_year_sales[block][y]) for y in late_years) / max(len(late_years), 1)
        vol_trend = math.log(max(late_vol, 0.5) / max(early_vol, 0.5))

        block_stats[block] = {
            "block":                block,
            "n_sales":              len(sales),
            "years_covered":        f"{start_yr}-{end_yr}",
            "cagr_price_per_sqft":  round(cagr * 100, 2) if cagr is not None else None,
            "median_price_per_sqft": round(end_med, 0),
            "median_dom":           round(statistics.median([s["dom"] for s in sales if s["dom"] is not None]), 1) if any(s["dom"] for s in sales) else None,
            "dom_trend":            round(dom_trend, 4) if dom_trend is not None else None,
            "median_sale_to_list":  round(median_stl, 4) if median_stl is not None else None,
            "median_assess_ratio":  round(median_assess_ratio, 4) if median_assess_ratio is not None else None,
            "vol_trend":            round(vol_trend, 4),
        }

    valid_blocks = {k: v for k, v in block_stats.items() if v is not None}
    print(f"  Blocks with enough data: {len(valid_blocks)} / {len(block_stats)}")

    # ── NORMALIZE & SCORE ────────────────────────────────────────────────
    print("\nNormalizing and scoring...")

    blocks_list = list(valid_blocks.values())

    cagr_norm   = percentile_rank_normalize([b["cagr_price_per_sqft"] for b in blocks_list])
    dom_norm    = percentile_rank_normalize([b["dom_trend"] for b in blocks_list], invert=True)
    stl_norm    = percentile_rank_normalize([b["median_sale_to_list"] for b in blocks_list])
    assess_norm = percentile_rank_normalize([b["median_assess_ratio"] for b in blocks_list])
    vol_norm    = percentile_rank_normalize([b["vol_trend"] for b in blocks_list], invert=True)

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

    # ── JOIN SCORES TO PROPERTIES ────────────────────────────────────────
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

    # ── WRITE OUTPUT FILES ───────────────────────────────────────────────
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

    write_csv(cleaned, data_dir / f"{prefix}_cleaned.csv")
    write_csv(scored,  data_dir / f"{prefix}_scored.csv")
    write_csv(ranked,  data_dir / f"{prefix}_block.csv")

    # ── SUMMARY ──────────────────────────────────────────────────────────
    print(f"\n{'='*60}")
    print(f"SUMMARY — {cfg['name']}")
    print(f"{'='*60}")
    print(f"Total properties cleaned:     {len(cleaned)}")
    print(f"Sold (arm's-length):          {len(sold)}")
    print(f"Blocks scored:                {len(ranked)}")

    key_types = Counter(b["block"].split("_")[0] for b in cleaned)
    print(f"Block key types:              {dict(key_types)}")

    park_high = sum(1 for r in cleaned if r["parking"] and r["parking_conf"] == "high")
    park_med  = sum(1 for r in cleaned if r["parking"] and r["parking_conf"] == "medium")
    park_none = sum(1 for r in cleaned if not r["parking"])
    print(f"Parking — high conf: {park_high}  medium: {park_med}  none/negative: {park_none}")

    print()
    print("TOP 10 BLOCKS BY APPRECIATION SCORE:")
    print(f"{'Rank':<5} {'Block':<25} {'Score':<8} {'CAGR%':<8} {'Med PSF':<10} {'Med DOM':<10} {'STL':<6} {'Sales'}")
    print("-" * 80)
    for b in ranked[:10]:
        print(f"{b['block_rank']:<5} {b['block']:<25} {b['appreciation_score']:<8} "
              f"{str(b['cagr_price_per_sqft'])+'%':<8} "
              f"${b['median_price_per_sqft']:<9,.0f} "
              f"{str(b['median_dom']):<10} "
              f"{b['median_sale_to_list']:<6} "
              f"{b['n_sales']}")

    print(f"\n{cfg['name']} cleaning done.")

    return {
        "cleaned": cleaned,
        "scored": scored,
        "ranked": ranked,
        "block_stats": block_stats,
    }


# ── CLI ────────────────────────────────────────────────────────────────────

def filter_rows_by_city(raw_rows, city_key):
    """Filter a combined CSV export to only rows matching this city."""
    cfg = CITY_CONFIGS[city_key]
    city_filter = cfg["city_filter"]
    return [r for r in raw_rows if r.get("City", "").strip() == city_filter]


def main():
    parser = argparse.ArgumentParser(
        description="Unified MLS data cleaning pipeline."
    )
    parser.add_argument(
        "--city", required=True,
        help="City key (jc_heights, hoboken, weehawken) or 'all' for combined export"
    )
    parser.add_argument(
        "--data-dir", type=Path, default=None,
        help="Data directory (default: city-specific under data/processed/)"
    )
    parser.add_argument(
        "--csv", type=Path, default=None,
        help="Input CSV path (overrides data_dir/Testing.csv)"
    )
    args = parser.parse_args()

    root = Path(__file__).resolve().parent.parent

    if args.city == "all":
        # Process a combined CSV that has all cities
        csv_path = args.csv
        if csv_path is None:
            print("ERROR: --csv required when --city=all", file=sys.stderr)
            sys.exit(1)

        print(f"Loading combined CSV: {csv_path}")
        with open(csv_path, encoding="utf-8-sig") as f:
            all_rows = list(csv.DictReader(f))
        print(f"  Total rows: {len(all_rows)}")

        # Auto-detect cities present
        city_counts = Counter(r.get("City", "").strip() for r in all_rows)
        print(f"  Cities found: {dict(city_counts)}")

        for city_name, city_key in CITY_COLUMN_MAP.items():
            if city_name not in city_counts:
                continue

            city_rows = filter_rows_by_city(all_rows, city_key)
            print(f"\n  {city_name}: {len(city_rows)} rows")

            data_dir = args.data_dir
            if data_dir is None:
                if city_key == "jc_heights":
                    data_dir = root / "data" / "processed"
                else:
                    data_dir = root / "data" / "processed" / city_key.replace("_", "")
            data_dir.mkdir(parents=True, exist_ok=True)

            run_clean(city_key, data_dir=data_dir, rows=city_rows)
    else:
        if args.city not in CITY_CONFIGS:
            print(f"ERROR: Unknown city '{args.city}'. Available: {list(CITY_CONFIGS.keys())}", file=sys.stderr)
            sys.exit(1)

        data_dir = args.data_dir
        if data_dir is None:
            if args.city == "jc_heights":
                data_dir = root / "data" / "processed"
            else:
                data_dir = root / "data" / "processed" / args.city.replace("_", "")

        run_clean(args.city, data_dir=data_dir, csv_path=args.csv)


if __name__ == "__main__":
    main()
