"""
JC Heights MLS Data Cleaning & Appreciation Scoring
----------------------------------------------------
Input:  Testing.csv  (Paragon MLS custom export)
Output: jc_heights_cleaned.csv   — cleaned, enriched dataset
        jc_heights_scored.csv    — per-property appreciation scores
        jc_heights_block.csv     — block-level market summary
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
OUT_CLEAN    = "jc_heights_cleaned.csv"
OUT_SCORED   = "jc_heights_scored.csv"
OUT_BLOCK    = "jc_heights_block.csv"

BLOCKSTREET_MIN  = 5     # min sales on a (block, street) pair to split into sub-key
CAGR_WINDOW_START = 2022  # standardised CAGR window — prevents open-ended span comparisons
CAGR_WINDOW_END   = 2025  # inclusive; fall back to full range if < 2 years in window

SCORE_WEIGHTS = {
    "block_cagr":          0.30,   # block-level price appreciation rate
    "dom_trend":           0.20,   # tightening days-on-market = demand pressure
    "sale_to_list_ratio":  0.20,   # selling over ask = competition
    "assess_delta":        0.15,   # market price outpacing assessed value = upside
    "inventory_trend":     0.15,   # fewer listings over time = supply squeeze
}

# ── HELPERS ──────────────────────────────────────────────────────────────────
def parse_price(s):
    """'$1,250,000' -> 1250000.0  |  blank/bad -> None"""
    if not s or not s.strip():
        return None
    cleaned = re.sub(r"[^\d.]", "", s.strip())
    try:
        v = float(cleaned)
        return v if v > 0 else None
    except ValueError:
        return None

def parse_date(s):
    """'10/13/2023' -> date object  |  blank/bad -> None"""
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

def is_residential(row):
    """Filter out parking spaces, storage units, commercial, etc."""
    beds = parse_int(row.get("Bedrooms", ""))
    price = parse_price(row.get("Sold Price", "") or row.get("Price", ""))
    unit = (row.get("Unit Number", "") or "").lower()
    addr = (row.get("Address", "") or "").lower()

    # Exclude obvious non-residential units
    non_res_keywords = ["parking", "storage", "garage", "commercial", "land"]
    for kw in non_res_keywords:
        if kw in unit or kw in addr:
            return False

    # Must have at least 1 bedroom or be a studio (0 beds but has sq ft)
    sqft = parse_int(row.get("Approx Sq Ft", ""))
    if beds == 0 and (sqft is None or sqft < 200):
        return False

    # Price sanity check — exclude anything under $75k (likely parking/error)
    if price is not None and price < 75_000:
        return False

    return True

blockstreet_counts: Counter = Counter()  # populated in pre-pass below before main loop

def block_key(row):
    """Use Block+Street as micro-market key when enough data; else Block; else zip.

    Tax blocks can span multiple streets/buildings, which distorts medians. When a
    (block, street) pair has >= BLOCKSTREET_MIN sales we create a finer-grained key
    (e.g. block_2801_PALISADE) so those comps stay in the same building cohort.
    """
    block = (row.get("Block", "") or "").strip()
    if not block:
        return f"zip_{(row.get('Zip','') or '').strip()}"
    addr = (row.get("Address", "") or "").strip()
    parts = addr.split()
    street_name = " ".join(parts[1:]) if len(parts) > 1 else ""
    if street_name and blockstreet_counts.get((block, street_name), 0) >= BLOCKSTREET_MIN:
        street_key = re.sub(r"[^A-Z0-9]", "", street_name.upper().split()[0])
        return f"block_{block}_{street_key}"
    return f"block_{block}"

def year_quarter(d):
    """date -> 'YYYY-Qn' string for time-series bucketing."""
    if d is None:
        return None
    q = (d.month - 1) // 3 + 1
    return f"{d.year}-Q{q}"

# ── STEP 1: LOAD & CLEAN ─────────────────────────────────────────────────────
print("Loading data...")
with open(INPUT_FILE, encoding="utf-8-sig") as f:
    raw = list(csv.DictReader(f))

print(f"  Raw rows: {len(raw)}")

# Pre-pass: count (block, street) occurrences so block_key() can split high-volume pairs
for _row in raw:
    if not is_residential(_row):
        continue
    _block = (_row.get("Block", "") or "").strip()
    if not _block:
        continue
    _addr = (_row.get("Address", "") or "").strip()
    _parts = _addr.split()
    _street = " ".join(_parts[1:]) if len(_parts) > 1 else ""
    if _street:
        blockstreet_counts[(_block, _street)] += 1
print(f"  Block+street pairs with >= {BLOCKSTREET_MIN} sales: "
      f"{sum(1 for v in blockstreet_counts.values() if v >= BLOCKSTREET_MIN)}")

# ── OPTIONAL ENRICHMENT LOOKUPS ──────────────────────────────────────────────
# These files are produced by the enrich_*.py scripts in scripts/ and are
# joined in below.  If a file doesn't exist the pipeline runs unchanged.

# Walk Score: keyed on MLS street address
walkscore_lu: dict = {}
ws_path = Path("address_walkscore.csv")
if ws_path.exists():
    with open(ws_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            walkscore_lu[_row["address"]] = _row
    print(f"  Walk Score data: {len(walkscore_lu)} addresses loaded")
else:
    print("  Walk Score data not found (run enrich_walkscore.py to generate)")

# MOD-IV: keyed on (block_raw_normalized, lot_normalized)
modiiv_lu: dict = {}
modiiv_path = Path("block_modiiv.csv")
if modiiv_path.exists():
    with open(modiiv_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            _key = (_row.get("block_raw", "").strip(), _row.get("lot", "").strip())
            modiiv_lu[_key] = _row
    print(f"  MOD-IV data: {len(modiiv_lu)} (block, lot) pairs loaded")
else:
    print("  MOD-IV data not found (run enrich_modiiv.py to generate)")

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

    # Derived fields
    price_per_sqft    = round(sold_price / sqft, 2) if sold_price and sqft and sqft > 0 else None
    sale_to_list      = round(sold_price / list_price, 4) if sold_price and list_price and list_price > 0 else None
    orig_to_sold_pct  = round((sold_price - orig_price) / orig_price * 100, 2) if sold_price and orig_price and orig_price > 0 else None
    assess_ratio      = round(sold_price / effective_assessed, 4) if sold_price and effective_assessed and effective_assessed > 0 else None
    total_baths       = (full_baths or 0) + 0.5 * (half_baths or 0)
    closing_year      = closing_date.year if closing_date else None
    closing_quarter   = year_quarter(closing_date)
    block             = block_key(row)

    # Floor number normalization
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

    # ── Enrichment: MOD-IV assessed value ────────────────────────────────
    # Normalize block/lot the same way enrich_modiiv.py does (strip leading zeros)
    _block_raw = row.get("Block", "").strip()
    _lot_raw   = row.get("Lot",   "").strip()
    _block_norm = str(int(_block_raw)) if _block_raw.isdigit() else _block_raw
    _lot_norm   = str(int(_lot_raw))   if _lot_raw.isdigit()   else _lot_raw
    _modiiv = modiiv_lu.get((_block_norm, _lot_norm), {})
    modiiv_assessed = parse_price(_modiiv.get("modiiv_assessed", "")) if _modiiv else None
    # Use MOD-IV assessed value when the MLS field is missing or zero
    effective_assessed = assessed_val or modiiv_assessed

    c = {
        "mls_id":           row.get("MLS #", "").strip(),
        "address":          row.get("Address", "").strip(),
        "unit_number":      row.get("Unit Number", "").strip(),
        "full_address":     f"{row.get('Address','').strip()} {row.get('Unit Number','').strip()}".strip(),
        "zip":              row.get("Zip", "").strip(),
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
        "lat":              parse_float(row.get("Y Coordinates", "")),
        "lon":              parse_float(row.get("X Coordinates", "")),
        "between":          row.get("Between", "").strip(),
        # Walk Score enrichment (None when enrich_walkscore.py hasn't been run)
        "walk_score":       walk_score,
        "transit_score":    transit_score,
        "bike_score":       bike_score,
        # MOD-IV enrichment
        "modiiv_assessed":  modiiv_assessed,   # official current-year assessment
        "assessed_value":   effective_assessed, # MLS value if available, else MOD-IV
        "assess_ratio":     assess_ratio,       # sale price / effective assessed value
    }
    cleaned.append(c)

print(f"  After filtering: {len(cleaned)} rows (skipped {skipped})")

# ── STEP 2: BLOCK-LEVEL MARKET ANALYSIS ─────────────────────────────────────
print("\nComputing block-level market signals...")

# Only use arm's-length (non-distressed) sold transactions with a closing date
sold = [r for r in cleaned if r["sold_price"] and r["closing_date"] and not r["is_distressed"]]

# Group by block
block_sales = defaultdict(list)
for r in sold:
    block_sales[r["block"]].append(r)

# Group by block AND year for trend analysis
block_year_sales = defaultdict(lambda: defaultdict(list))
for r in sold:
    if r["closing_year"]:
        block_year_sales[r["block"]][r["closing_year"]].append(r)

block_stats = {}
MIN_SALES = 5  # minimum sales needed to compute block stats

for block, sales in block_sales.items():
    years = sorted(block_year_sales[block].keys())

    if len(years) < 2 or len(sales) < MIN_SALES:
        block_stats[block] = None
        continue

    # Median price per sqft by year (more robust than raw price)
    year_medians = {}
    for yr in years:
        yr_sales = block_year_sales[block][yr]
        psf = [s["price_per_sqft"] for s in yr_sales if s["price_per_sqft"]]
        if psf:
            year_medians[yr] = statistics.median(psf)

    if len(year_medians) < 2:
        block_stats[block] = None
        continue

    # CAGR over a standardised window so blocks are comparable.
    # Prefer CAGR_WINDOW_START–END; fall back to full range only if < 2 window years.
    yr_list = sorted(year_medians.keys())
    window_medians = {yr: med for yr, med in year_medians.items()
                      if CAGR_WINDOW_START <= yr <= CAGR_WINDOW_END}
    if len(window_medians) >= 2:
        start_yr  = min(window_medians.keys())
        end_yr    = max(window_medians.keys())
        start_med = window_medians[start_yr]
        end_med   = window_medians[end_yr]
    else:
        # Fall back to full range — mark via cagr_windowed=False downstream if needed
        start_yr  = yr_list[0]
        end_yr    = yr_list[-1]
        start_med = year_medians[start_yr]
        end_med   = year_medians[end_yr]
    n_years = end_yr - start_yr

    if n_years > 0 and start_med > 0:
        cagr = (end_med / start_med) ** (1 / n_years) - 1
    else:
        cagr = None

    # Median DOM by year — tightening = demand signal
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
        # Log-ratio avoids +1 smoothing distortion on low-DOM blocks (e.g. 3d→10d
        # reads as +333% with / (early+1) but only +log(3.3)≈1.2 with log-ratio).
        # Negative = DOM shrinking = good.
        dom_trend = math.log(max(late_dom, 1.0) / max(early_dom, 1.0))

    # Median sale-to-list ratio — above 1.0 = selling over ask
    stl_ratios = [s["sale_to_list"] for s in sales if s["sale_to_list"] is not None]
    median_stl = statistics.median(stl_ratios) if stl_ratios else None

    # Assessment ratio — how far above assessed value are sales going?
    assess_ratios = [s["assess_ratio"] for s in sales if s["assess_ratio"] is not None]
    median_assess_ratio = statistics.median(assess_ratios) if assess_ratios else None

    # Inventory trend — count of sales per year (proxy for supply)
    early_years = yr_list[:len(yr_list)//2]
    late_years  = yr_list[len(yr_list)//2:]
    early_vol   = sum(len(block_year_sales[block][y]) for y in early_years) / max(len(early_years),1)
    late_vol    = sum(len(block_year_sales[block][y]) for y in late_years)  / max(len(late_years),1)
    # Log-ratio: symmetric and unaffected by scale. Negative = supply shrinking = good.
    vol_trend = math.log(max(late_vol, 0.5) / max(early_vol, 0.5))

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
    """Percentile-rank normalize a list to [0, 1].

    More robust than min-max for small samples: a single outlier sale can't
    push all other blocks to near-zero or near-one. Ties get the same rank.
    invert=True for lower-is-better metrics (e.g. DOM trend, inventory trend).
    """
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
            # Midpoint rank: average of lower and upper bound positions → [0, 1]
            lo = bisect.bisect_left(non_null, v)
            hi = bisect.bisect_right(non_null, v)
            rank = (lo + hi) / 2 / n
            result[i] = (1.0 - rank) if invert else rank
    return result

blocks_list = list(valid_blocks.values())

cagr_norm    = normalize([b["cagr_price_per_sqft"] for b in blocks_list])
dom_norm     = normalize([b["dom_trend"] for b in blocks_list], invert=True)   # lower DOM trend = better
stl_norm     = normalize([b["median_sale_to_list"] for b in blocks_list])
assess_norm  = normalize([b["median_assess_ratio"] for b in blocks_list])
vol_norm     = normalize([b["vol_trend"] for b in blocks_list], invert=True)   # declining supply = better

for i, b in enumerate(blocks_list):
    scores = {
        "block_cagr":         cagr_norm[i],
        "dom_trend":          dom_norm[i],
        "sale_to_list_ratio": stl_norm[i],
        "assess_delta":       assess_norm[i],
        "inventory_trend":    vol_norm[i],
    }
    weighted = sum(
        SCORE_WEIGHTS[k] * v
        for k, v in scores.items()
        if v is not None
    )
    total_weight = sum(
        SCORE_WEIGHTS[k]
        for k, v in scores.items()
        if v is not None
    )
    b["appreciation_score"] = round(weighted / total_weight * 100, 1) if total_weight > 0 else None
    b.update({f"score_{k}": round(v * 100, 1) if v is not None else None for k, v in scores.items()})

# Rank blocks
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
print()
print("TOP 10 BLOCKS BY APPRECIATION SCORE:")
print(f"{'Rank':<5} {'Block':<15} {'Score':<8} {'CAGR%':<8} {'Med PSF':<10} {'Med DOM':<10} {'STL':<6} {'Sales'}")
print("-"*70)
for b in ranked[:10]:
    print(f"{b['block_rank']:<5} {b['block']:<15} {b['appreciation_score']:<8} "
          f"{str(b['cagr_price_per_sqft'])+'%':<8} "
          f"${b['median_price_per_sqft']:<9,.0f} "
          f"{str(b['median_dom']):<10} "
          f"{b['median_sale_to_list']:<6} "
          f"{b['n_sales']}")

print("\nDone.")
