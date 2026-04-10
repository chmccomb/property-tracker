"""
JC Heights — Emerging Appreciation Score
-----------------------------------------
Adds a forward-looking "Emerging" score to each block alongside the
existing "Established" score. Also enriches properties with lat/lon
from the geocache and writes final dashboard-ready JSON.

Signals in the Emerging score:
  1. Relative undervaluation   (30%) — block $/sqft vs. zip-level median
  2. Recent acceleration       (30%) — last-12-month CAGR vs. 5-yr CAGR
  3. DOM tightening (recent)   (20%) — DOM change in last 4 qtrs vs. prior
  4. Low price, rising demand  (20%) — affordable + stl ratio trending up
"""

import csv, json, math, statistics
from datetime import date, datetime, timedelta
from collections import defaultdict
from pathlib import Path

# ── LOAD DATA ────────────────────────────────────────────────────────────
with open('jc_heights_cleaned.csv') as f:
    props = list(csv.DictReader(f))

with open('jc_heights_block.csv') as f:
    blocks = list(csv.DictReader(f))

# Load geocode cache produced by geocode.py (lat/lon already joined by clean.py,
# but we also need it here for the radius-based comp fallback spatial index).
geocache: dict = {}
_geo_path = Path("address_geocoded.csv")
if _geo_path.exists():
    with open(_geo_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            if _row.get("lat") and _row.get("lon"):
                try:
                    geocache[_row["address"]] = {
                        "lat": float(_row["lat"]), "lon": float(_row["lon"])
                    }
                except (ValueError, TypeError):
                    pass
    print(f"Geocache loaded: {len(geocache)} addresses")
else:
    print("No geocache found — proceeding without coordinates")

# ── OPTIONAL ENRICHMENT: Census ACS neighborhood data ────────────────────────
# Produced by scripts/enrich_census.py; keyed on ZIP code.
# Provides neighborhood_demand_score and demographic signals per zip.
census_lu: dict = {}
census_path = Path("zip_census.csv")
if census_path.exists():
    with open(census_path, encoding="utf-8") as _f:
        for _row in csv.DictReader(_f):
            census_lu[_row["zip"]] = _row
    print(f"Census ACS data: {len(census_lu)} ZIP codes loaded")
else:
    print("Census ACS data not found (run enrich_census.py to generate)")

# ── OPEN HOUSE SCHEDULE ───────────────────────────────────────────────────
open_house_lu: dict = {}
oh_path = Path("open_houses.csv")
if oh_path.exists():
    with open(oh_path, encoding="utf-8-sig") as _f:
        for _row in csv.DictReader(_f):
            mls = _row.get("MLS #", "").strip()
            d   = _row.get("Tour/Open House - Start Date", "").strip()
            s   = _row.get("Tour/Open House - Start Time", "").strip()
            e   = _row.get("Tour/Open House - End Time",   "").strip()
            if mls and d:
                open_house_lu.setdefault(mls, []).append({"date": d, "start": s, "end": e})
    print(f"Open houses: {sum(len(v) for v in open_house_lu.values())} sessions "
          f"across {len(open_house_lu)} listings")
else:
    print("Open house data not found (copy Tour_Open_House_Thumbnail.csv → open_houses.csv)")

# ── HELPERS ──────────────────────────────────────────────────────────────
def flt(v):
    if v in (None, '', 'None'): return None
    try: return float(v)
    except: return None

def dt(s):
    if not s or s == 'None': return None
    try: return date.fromisoformat(s)
    except: return None

def median(lst):
    lst = [x for x in lst if x is not None]
    if not lst: return None
    return statistics.median(lst)

def normalize_list(values, invert=False):
    """Percentile-rank normalization [0, 1]. Robust to outliers in small block samples.

    Replaces min-max: a single extreme block no longer compresses all others toward
    the center. Ties get the same rank. invert=True for lower-is-better signals.
    """
    import bisect
    non_null = sorted([v for v in values if v is not None])
    n = len(non_null)
    if n == 0:
        return [None] * len(values)
    out = []
    for v in values:
        if v is None:
            out.append(None)
        elif n == 1:
            out.append(0.5)
        else:
            lo   = bisect.bisect_left(non_null, v)
            hi   = bisect.bisect_right(non_null, v)
            rank = (lo + hi) / 2 / n   # midpoint rank in [0, 1]
            out.append(1.0 - rank if invert else rank)
    return out

today = date.today()
cutoff_recent = today - timedelta(days=365)      # last 12 months
cutoff_mid    = today - timedelta(days=365*2)    # 12–24 months ago

# ── FILTER TO ARM'S-LENGTH SOLD ─────────────────────────────────────────
sold = []
for p in props:
    if p.get('status') != 'SOLD': continue
    if p.get('is_distressed') == 'True': continue
    d = dt(p.get('closing_date'))
    psf = flt(p.get('price_per_sqft'))
    if d and psf and psf > 50:
        p['_closing_date'] = d
        p['_psf'] = psf
        sold.append(p)

print(f"Arm's-length sold: {len(sold)}")

# ── SPATIAL INDEX FOR RADIUS-BASED COMP FALLBACK ─────────────────────────
# Used when a property's block has < RADIUS_MIN_BLOCK_SALES sold transactions.
# Find all arm's-length sold comps within RADIUS_MILES and take median PSF.

RADIUS_MILES          = 0.30   # ~5-minute walk; ~4–6 blocks in the Heights grid
RADIUS_MIN_COMPS      = 5      # need at least this many comps for a valid estimate
RADIUS_MIN_BLOCK_SALES = 10    # use radius fallback when block has fewer than this

def _haversine_miles(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = math.sin(dlat/2)**2 + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2)) * math.sin(dlon/2)**2
    return R * 2 * math.asin(math.sqrt(a))

# Build list of (lat, lon, psf, property_type) for all geocoded sold comps
_spatial_comps = []
for _p in sold:
    _geo = geocache.get(_p.get('address', ''))
    if _geo and _p.get('_psf'):
        _spatial_comps.append((_geo['lat'], _geo['lon'], _p['_psf'], _p.get('property_type', '')))

print(f"Spatial comp index: {len(_spatial_comps)} geocoded sold properties")

def radius_median_psf(lat, lon, prop_type=''):
    """Return (median_psf, n_comps) for sold comps within RADIUS_MILES.
    Prefers same property type; falls back to all types if too few same-type comps."""
    same_type = [psf for clat, clon, psf, ctype in _spatial_comps
                 if ctype == prop_type and _haversine_miles(lat, lon, clat, clon) <= RADIUS_MILES]
    if len(same_type) >= RADIUS_MIN_COMPS:
        return statistics.median(same_type), len(same_type)
    all_nearby = [psf for clat, clon, psf, _ in _spatial_comps
                  if _haversine_miles(lat, lon, clat, clon) <= RADIUS_MILES]
    if len(all_nearby) >= RADIUS_MIN_COMPS:
        return statistics.median(all_nearby), len(all_nearby)
    return None, 0

# ── ZIP-LEVEL MEDIAN PSF (benchmark for undervaluation) ──────────────────
zip_psfs = defaultdict(list)
for p in sold:
    zip_psfs[p['zip']].append(p['_psf'])
zip_median_psf = {z: median(psfs) for z, psfs in zip_psfs.items()}
print(f"Zip medians: {dict(zip_median_psf)}")

# ── BLOCK → ZIP LOOKUP (for dynamic affordability threshold) ─────────────
# First zip seen per block — used to pull the right zip-level median PSF.
block_zip: dict = {}
for p in sold:
    if p['block'] not in block_zip:
        block_zip[p['block']] = p['zip']

# ── BLOCK-LEVEL TIME-SERIES DATA ─────────────────────────────────────────
block_recent  = defaultdict(list)   # last 12 mo
block_mid     = defaultdict(list)   # 12–24 mo ago
block_prior   = defaultdict(list)   # 24+ mo ago (for DOM comparison)
block_all_psf = defaultdict(list)
block_dom_recent = defaultdict(list)
block_dom_prior  = defaultdict(list)
block_stl_recent = defaultdict(list)
block_stl_prior  = defaultdict(list)

for p in sold:
    bk  = p['block']
    d   = p['_closing_date']
    psf = p['_psf']
    dom = flt(p.get('dom'))
    stl = flt(p.get('sale_to_list'))

    block_all_psf[bk].append(psf)

    if d >= cutoff_recent:
        block_recent[bk].append(psf)
        if dom is not None: block_dom_recent[bk].append(dom)
        if stl is not None: block_stl_recent[bk].append(stl)
    elif d >= cutoff_mid:
        block_mid[bk].append(psf)
    else:
        block_prior[bk].append(psf)
        if dom is not None: block_dom_prior[bk].append(dom)
        if stl is not None: block_stl_prior[bk].append(stl)

# ── COMPUTE EMERGING SIGNALS PER BLOCK ──────────────────────────────────
MIN_RECENT = 3   # min sales in last 12 mo to compute recent signals

emerging_raw = {}

for bk in set(p['block'] for p in sold):
    rec  = block_recent.get(bk, [])
    mid  = block_mid.get(bk, [])
    pri  = block_prior.get(bk, [])
    all_ = block_all_psf.get(bk, [])

    # Need at least some recent activity
    if len(rec) < MIN_RECENT and len(mid) < MIN_RECENT:
        emerging_raw[bk] = None
        continue

    med_psf_all    = median(all_)
    med_psf_recent = median(rec) if rec else median(mid)
    med_psf_prior  = median(pri) if pri else None

    # 1. RELATIVE UNDERVALUATION
    # How far below zip-level median is this block's recent price?
    zip_code = block_zip.get(bk, '07307')
    zip_med  = zip_median_psf.get(zip_code)
    if zip_med and med_psf_recent:
        # Positive = block cheaper than zip avg = undervalued = emerging opportunity
        underval = (zip_med - med_psf_recent) / zip_med
    else:
        underval = None

    # 2. RECENT ACCELERATION
    # Compare last-12-mo median PSF to prior-period median PSF
    if med_psf_recent and med_psf_prior and med_psf_prior > 0:
        acceleration = (med_psf_recent - med_psf_prior) / med_psf_prior
    elif med_psf_recent and med_psf_all and med_psf_all > 0:
        # Fallback: compare recent to overall average
        acceleration = (med_psf_recent - med_psf_all) / med_psf_all
    else:
        acceleration = None

    # 3. DOM TIGHTENING (recent vs. prior)
    dom_rec = median(block_dom_recent.get(bk, []))
    dom_pri = median(block_dom_prior.get(bk, []))
    if dom_rec is not None and dom_pri is not None and dom_pri > 0:
        # Negative = DOM shrinking = demand building = good
        dom_tightening = (dom_rec - dom_pri) / dom_pri
    else:
        dom_tightening = None

    # 4. DEMAND BUILDING (stl trend + affordability combo)
    stl_rec = median(block_stl_recent.get(bk, []))
    stl_pri = median(block_stl_prior.get(bk, []))
    if stl_rec is not None and stl_pri is not None and stl_pri > 0:
        stl_trend = (stl_rec - stl_pri) / stl_pri
    else:
        stl_trend = None

    # Affordability: how far below the zip-level median PSF is this block?
    # Uses the dynamic zip median instead of a hardcoded $700 threshold so the
    # signal stays valid as market prices change over time.
    zip_thresh = zip_med or 700  # fallback to 700 only if zip median unavailable
    affordability = max(0, (zip_thresh - (med_psf_recent or zip_thresh)) / zip_thresh)

    demand_signal = None
    if stl_trend is not None:
        demand_signal = stl_trend * 0.6 + affordability * 0.4
    else:
        demand_signal = affordability

    emerging_raw[bk] = {
        'block':            bk,
        'underval':         underval,
        'acceleration':     acceleration,
        'dom_tightening':   dom_tightening,
        'demand_signal':    demand_signal,
        'med_psf_recent':   round(med_psf_recent, 0) if med_psf_recent else None,
        'med_psf_prior':    round(med_psf_prior, 0) if med_psf_prior else None,
        'dom_recent':       round(dom_rec, 1) if dom_rec else None,
        'dom_prior':        round(dom_pri, 1) if dom_pri else None,
        'stl_recent':       round(stl_rec, 4) if stl_rec else None,
        'n_recent':         len(rec),
        'n_prior':          len(pri),
    }

valid = {k: v for k, v in emerging_raw.items() if v is not None}
print(f"Blocks with emerging data: {len(valid)}")

# ── NORMALIZE & SCORE ────────────────────────────────────────────────────
bk_list = list(valid.values())

weights = {'underval': 0.30, 'acceleration': 0.30, 'dom_tightening': 0.20, 'demand_signal': 0.20}

underval_n   = normalize_list([b['underval']       for b in bk_list])           # higher = more undervalued = better
accel_n      = normalize_list([b['acceleration']    for b in bk_list])           # higher = faster recent growth
dom_n        = normalize_list([b['dom_tightening']  for b in bk_list], invert=True)  # lower (shrinking) = better
demand_n     = normalize_list([b['demand_signal']   for b in bk_list])

for i, b in enumerate(bk_list):
    scores = {
        'underval':      underval_n[i],
        'acceleration':  accel_n[i],
        'dom_tightening':dom_n[i],
        'demand_signal': demand_n[i],
    }
    weighted = sum(weights[k] * v for k, v in scores.items() if v is not None)
    total_w  = sum(weights[k]     for k, v in scores.items() if v is not None)
    base_score = round(weighted / total_w * 100, 1) if total_w >= 0.5 else None

    # Interaction bonus: +5 pts when both undervalued AND accelerating.
    # These signals are most predictive together — cheap AND rising means momentum
    # hasn't been priced in yet. Capped at 100.
    interaction_bonus = 0
    if (b.get('underval') is not None and b.get('acceleration') is not None
            and b['underval'] > 0 and b['acceleration'] > 0):
        interaction_bonus = 5

    b['emerging_score'] = min(100, base_score + interaction_bonus) if base_score is not None else None
    b['s_underval']     = round(underval_n[i] * 100, 1) if underval_n[i] is not None else None
    b['s_accel']        = round(accel_n[i]    * 100, 1) if accel_n[i]    is not None else None
    b['s_dom_tight']    = round(dom_n[i]      * 100, 1) if dom_n[i]      is not None else None
    b['s_demand']       = round(demand_n[i]   * 100, 1) if demand_n[i]   is not None else None

# Rank
ranked_emerging = sorted([b for b in bk_list if b['emerging_score'] is not None],
                          key=lambda x: x['emerging_score'], reverse=True)
for i, b in enumerate(ranked_emerging):
    b['emerging_rank'] = i + 1

# ── MERGE WITH ESTABLISHED SCORES ───────────────────────────────────────
established = {b['block']: b for b in blocks}
emerging_lu  = {b['block']: b for b in bk_list}

merged_blocks = []
all_block_keys = set(list(established.keys()) + list(emerging_lu.keys()))

for bk in all_block_keys:
    est = established.get(bk, {})
    emg = emerging_lu.get(bk, {})

    # Census data — look up by the block's zip code
    _zip = block_zip.get(bk, '')
    cen = census_lu.get(_zip, {})

    merged_blocks.append({
        'block':            bk,
        'block_raw':        bk.replace('block_',''),
        'n_sales':          flt(est.get('n_sales')) or emg.get('n_recent',0) + emg.get('n_prior',0),
        'years':            est.get('years_covered',''),
        # Established
        'est_score':        flt(est.get('appreciation_score')),
        'est_rank':         flt(est.get('block_rank')),
        'cagr':             flt(est.get('cagr_price_per_sqft')),
        'med_psf':          flt(est.get('median_price_per_sqft')),
        'med_dom':          flt(est.get('median_dom')),
        'med_stl':          flt(est.get('median_sale_to_list')),
        'est_s_cagr':       flt(est.get('score_block_cagr')),
        'est_s_dom':        flt(est.get('score_dom_trend')),
        'est_s_stl':        flt(est.get('score_sale_to_list_ratio')),
        'est_s_assess':     flt(est.get('score_assess_delta')),
        'est_s_inv':        flt(est.get('score_inventory_trend')),
        # Emerging
        'emg_score':        emg.get('emerging_score'),
        'emg_rank':         emg.get('emerging_rank'),
        'med_psf_recent':   emg.get('med_psf_recent'),
        'med_psf_prior':    emg.get('med_psf_prior'),
        'acceleration':     round(emg['acceleration']*100,1) if emg.get('acceleration') is not None else None,
        'underval_pct':     round(emg['underval']*100,1) if emg.get('underval') is not None else None,
        'dom_recent':       emg.get('dom_recent'),
        'dom_prior':        emg.get('dom_prior'),
        'stl_recent':       emg.get('stl_recent'),
        'n_recent':         emg.get('n_recent',0),
        'emg_s_underval':   emg.get('s_underval'),
        'emg_s_accel':      emg.get('s_accel'),
        'emg_s_dom_tight':  emg.get('s_dom_tight'),
        'emg_s_demand':     emg.get('s_demand'),
        # Census ACS neighborhood signals (None when enrich_census.py hasn't been run)
        'neighborhood_demand': flt(cen.get('neighborhood_demand_score')),
        'median_income':       flt(cen.get('B19013_001E')),
        'vacancy_rate_pct':    flt(cen.get('vacancy_rate_pct')),
        'renter_rate_pct':     flt(cen.get('renter_rate_pct')),
        'acs_year':            cen.get('acs_year', ''),
    })

# ── ENRICH PROPERTIES WITH GEOCOORDS + EMERGING SCORE ───────────────────
emg_lookup = {b['block']: b for b in merged_blocks}

_radius_used = _radius_skipped = 0

enriched_props = []
for p in props:
    r = dict(p)
    addr = r.get('address','')

    # lat/lon: prefer geocache (authoritative), fall back to whatever clean.py set
    geo = geocache.get(addr)
    r['lat'] = geo['lat'] if geo else (flt(r.get('lat')) or None)
    r['lon'] = geo['lon'] if geo else (flt(r.get('lon')) or None)

    bdata = emg_lookup.get(r.get('block',''), {})
    r['est_score'] = bdata.get('est_score')
    r['emg_score'] = bdata.get('emg_score')
    r['est_rank']  = bdata.get('est_rank')
    r['emg_rank']  = bdata.get('emg_rank')
    r['block_cagr_pct']  = bdata.get('cagr')
    r['block_median_dom'] = bdata.get('med_dom')
    r['block_median_stl'] = bdata.get('med_stl')
    r['block_n_sales']    = bdata.get('n_sales')

    # ── Radius-based comp fallback ────────────────────────────────────────
    # Use block PSF when reliable; fall back to radius median when block is
    # thin (< RADIUS_MIN_BLOCK_SALES) or missing entirely.
    block_psf   = bdata.get('med_psf')
    block_n     = flt(bdata.get('n_sales')) or 0
    rad_psf     = None
    rad_n       = 0
    psf_source  = 'block'

    if (not block_psf or block_n < RADIUS_MIN_BLOCK_SALES) and r['lat'] and r['lon']:
        rad_psf, rad_n = radius_median_psf(r['lat'], r['lon'], r.get('property_type',''))
        if rad_psf:
            _radius_used += 1
            psf_source = 'radius'
        else:
            _radius_skipped += 1
    elif not block_psf:
        _radius_skipped += 1

    r['block_median_psf'] = block_psf or rad_psf   # prefer block; use radius when block absent/thin
    r['radius_median_psf'] = rad_psf
    r['radius_n_comps']    = rad_n if rad_psf else None
    r['psf_source']        = psf_source
    enriched_props.append(r)

print(f"PSF source — block: {len(enriched_props)-_radius_used-_radius_skipped}  "
      f"radius fallback: {_radius_used}  no data: {_radius_skipped}")

# ── WRITE UPDATED BLOCK CSV ──────────────────────────────────────────────
with open('jc_heights_blocks_v2.csv', 'w', newline='') as f:
    w = csv.DictWriter(f, fieldnames=merged_blocks[0].keys())
    w.writeheader(); w.writerows(merged_blocks)
print(f"Wrote {len(merged_blocks)} blocks -> jc_heights_blocks_v2.csv")

# ── BUILD DASHBOARD JSON ─────────────────────────────────────────────────
def to_num(v):
    if v in (None, '', 'None', 'True', 'False'):
        if v == 'True': return True
        if v == 'False': return False
        return None
    try: return float(v)
    except: return v

# Per-block year trend (PSF by year)
block_trends = defaultdict(lambda: defaultdict(list))
for p in sold:
    yr = p.get('closing_year')
    if yr and p['_psf']:
        block_trends[p['block']][yr].append(p['_psf'])

trends_out = {}
for bk, yrs in block_trends.items():
    trends_out[bk] = {yr: round(statistics.median(psfs),0) for yr, psfs in sorted(yrs.items()) if psfs}

# Overall market trend
overall_trend = {}
yr_psf = defaultdict(list)
for p in sold:
    if p.get('closing_year') and p['_psf']:
        yr_psf[p['closing_year']].append(p['_psf'])
for yr, psfs in sorted(yr_psf.items()):
    overall_trend[yr] = round(statistics.median(psfs),0)

# Props for JSON
props_json = []
for p in enriched_props:
    props_json.append({
        'mls':        p.get('mls_id',''),
        'address':    p.get('address',''),
        'unit':       p.get('unit_number',''),
        'complex':    p.get('complex_name',''),
        'type':       p.get('property_type',''),
        'beds':       to_num(p.get('bedrooms')),
        'baths':      to_num(p.get('total_baths')),
        'sqft':       to_num(p.get('sqft')),
        'sold_price': to_num(p.get('sold_price')),
        'list_price': to_num(p.get('list_price')),
        'psf':        to_num(p.get('price_per_sqft')),
        'stl':        to_num(p.get('sale_to_list')),
        'dom':        to_num(p.get('dom')),
        'closing_date': p.get('closing_date',''),
        'closing_year': to_num(p.get('closing_year')),
        'hoa':        to_num(p.get('hoa_monthly')),
        'taxes':      to_num(p.get('taxes')),
        'assessed':   to_num(p.get('assessed_value')),
        'assess_ratio': to_num(p.get('assess_ratio')),
        'block':      p.get('block',''),
        'block_raw':  p.get('block_raw',''),
        'est_score':  to_num(p.get('est_score')),
        'emg_score':  to_num(p.get('emg_score')),
        'est_rank':   to_num(p.get('est_rank')),
        'emg_rank':   to_num(p.get('emg_rank')),
        'block_cagr': to_num(p.get('block_cagr_pct')),
        'block_psf':       to_num(p.get('block_median_psf')),
        'radius_psf':      to_num(p.get('radius_median_psf')),
        'radius_n_comps':  to_num(p.get('radius_n_comps')),
        'psf_source':      p.get('psf_source', 'block'),
        'block_dom':  to_num(p.get('block_median_dom')),
        'block_stl':  to_num(p.get('block_median_stl')),
        'block_n':    to_num(p.get('block_n_sales')),
        'parking':      p.get('parking') == 'True',
        'parking_conf': p.get('parking_conf',''),
        'outdoor':      p.get('outdoor') == 'True',
        'outdoor_type': p.get('outdoor_type',''),
        'outdoor_conf': p.get('outdoor_conf',''),
        'distressed': p.get('is_distressed') == 'True',
        'status':     p.get('status',''),
        'lat':        to_num(p.get('lat')),
        'lon':        to_num(p.get('lon')),
        # Walk Score enrichment
        'walk_score':    to_num(p.get('walk_score')),
        'transit_score': to_num(p.get('transit_score')),
        'bike_score':    to_num(p.get('bike_score')),
        # Transit proximity
        'path_station':    p.get('path_station') or None,
        'path_dist_mi':    to_num(p.get('path_dist_mi')),
        'hblr_station':    p.get('hblr_station') or None,
        'hblr_dist_mi':    to_num(p.get('hblr_dist_mi')),
        'transit_station': p.get('transit_station') or None,
        'transit_dist_mi': to_num(p.get('transit_dist_mi')),
        # MOD-IV
        'modiiv_assessed': to_num(p.get('modiiv_assessed')),
        'open_houses':     open_house_lu.get(p.get('mls_id', ''), []),
    })

# Blocks for JSON
blocks_json = []
for b in merged_blocks:
    blocks_json.append({k: (round(v,3) if isinstance(v,float) else v) for k,v in b.items()})

dashboard = {
    'properties':   props_json,
    'blocks':       blocks_json,
    'trends':       trends_out,
    'overall_trend': overall_trend,
}

with open('dashboard_data_v2.json','w') as f:
    json.dump(dashboard, f, separators=(',',':'))

sz = len(json.dumps(dashboard)) / 1024
print(f"Dashboard JSON: {sz:.0f} KB")

# ── PRINT EMERGING TOP 10 ────────────────────────────────────────────────
print("\nTOP 10 EMERGING BLOCKS:")
print(f"{'Rank':<5} {'Block':<12} {'Emg':<6} {'Est':<6} {'Accel%':<9} {'Underval%':<11} {'PSF Recent':<12} {'DOM tight'}")
print("-"*75)
for b in ranked_emerging[:10]:
    bm = emg_lookup.get(b['block'],{})
    print(f"{b['emerging_rank']:<5} {b['block'].replace('block_',''):<12} "
          f"{b['emerging_score']:<6} "
          f"{str(round(float(established.get(b['block'],{}).get('appreciation_score') or 0))):<6} "
          f"{str(round(b['acceleration']*100,1) if b['acceleration'] else 0)+'%':<9} "
          f"{str(round(b['underval']*100,1) if b['underval'] else 0)+'%':<11} "
          f"${b['med_psf_recent'] or 0:<11,.0f} "
          f"{round(b['dom_tightening']*100,1) if b['dom_tightening'] else '—'}")
