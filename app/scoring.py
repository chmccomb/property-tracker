"""Server-side deal scoring.

Port of computeDealScore / blockQualityScore / getBedMatchedBlockPsf from
dashboard/unified_dashboard.html. Living here (not the dashboard) means
the scoring formula + market-specific constants never ship to the
client — the gate that makes free/paid tiers meaningful.

Public:
    compute_deal_score(prop, all_sold, city_key) -> dict | None
    score_active_listings(session, city_key) -> list[dict]
"""

import math
from datetime import date, timedelta
from statistics import median
from typing import Optional

from sqlalchemy.orm import Session

from app.models import Property

# Per-city scoring constants ported from dashboard MARKET_META.
# Keys match CITY_CONFIGS keys (jc_heights, hoboken, weehawken).
MARKET_META = {
    "jc_heights": {
        "flag_park": 0.40, "flag_out": 0.55,
        "parking_psf_pct": 8.3, "outdoor_psf_pct": 7.9, "both_psf_pct": 13.7,
    },
    "hoboken": {
        "flag_park": 0.37, "flag_out": 0.60,
        "parking_psf_pct": 3.5, "outdoor_psf_pct": 2.3, "both_psf_pct": 5.5,
    },
    "weehawken": {
        "flag_park": 0.43, "flag_out": 0.50,
        "parking_psf_pct": 6.0, "outdoor_psf_pct": 4.0, "both_psf_pct": 9.0,
    },
}
DEFAULT_META = MARKET_META["jc_heights"]

# Cap-rate used to convert HOA into a price-equivalent reduction.
HOA_CAP_RATE = 0.07


def _haversine_mi(lat1, lon1, lat2, lon2):
    R = 3958.8
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(a))


def block_quality_score(est, emg, n_sales):
    """Blend Established (35%) + Emerging (65%), with thin-block discount."""
    if est is not None and emg is not None:
        bq = est * 0.35 + emg * 0.65
    elif est is not None:
        bq = est
    elif emg is not None:
        bq = emg
    else:
        return None
    # Thin-block confidence: blend toward neutral 50 when < 10 sales.
    if n_sales is not None and n_sales < 10:
        conf = max(0.5, n_sales / 10.0)
        bq = bq * conf + 50.0 * (1.0 - conf)
    return bq


def get_bed_matched_psf(all_sold, block, beds, fallback,
                        lat=None, lon=None, is_zip=False):
    """Find PSF from comps matching beds: 24mo → 36mo → overall fallback."""
    today = date.today()
    cutoff24 = (today - timedelta(days=365 * 2)).isoformat()
    cutoff36 = (today - timedelta(days=365 * 3)).isoformat()

    def nearby(p, cutoff_str):
        psf = p.get("psf")
        if not psf or psf <= 0 or p.get("status") != "SOLD":
            return False
        if (p.get("closing_date") or "") < cutoff_str:
            return False
        if beds is not None and p.get("beds") != beds:
            return False
        if is_zip and lat and lon and p.get("lat") and p.get("lon"):
            return _haversine_mi(lat, lon, p["lat"], p["lon"]) <= 0.35
        return p.get("block") == block

    comps = [p for p in all_sold if nearby(p, cutoff24)]
    if len(comps) >= 4:
        return {"psf": median(p["psf"] for p in comps),
                "n": len(comps), "source": "bed-matched 24mo"}

    comps = [p for p in all_sold if nearby(p, cutoff36)]
    if len(comps) >= 3:
        return {"psf": median(p["psf"] for p in comps),
                "n": len(comps), "source": "bed-matched 36mo"}

    return {"psf": fallback, "n": None, "source": "block overall median"}


def compute_deal_score(prop, all_sold, city_key):
    """Return {deal, price_pct, adj_block_psf, ref_psf_source, hoa_adj_psf,
    using_radius} or None if not scorable."""
    psf = prop.get("psf")
    comp_psf = prop.get("block_psf") or prop.get("radius_psf")
    if not psf or not comp_psf:
        return None

    block = prop.get("block") or ""
    # dashboard uses prefixed "{market}/zip_..." — server uses unprefixed
    is_zip = block.startswith("zip_") or "/zip_" in block

    bq = block_quality_score(
        None if is_zip else prop.get("est_score"),
        None if is_zip else prop.get("emg_score"),
        None if is_zip else prop.get("block_n"),
    )
    if bq is None:
        return None

    meta = MARKET_META.get(city_key, DEFAULT_META)
    beds = prop.get("beds")
    park_pct = meta["parking_psf_pct"] if prop.get("parking") else 0
    out_pct = meta["outdoor_psf_pct"] if prop.get("outdoor") else 0
    combo_pct = (min(park_pct + out_pct, meta["both_psf_pct"])
                 if prop.get("parking") and prop.get("outdoor")
                 else park_pct + out_pct)
    block_avg_prem = (meta["flag_park"] * meta["parking_psf_pct"]
                      + meta["flag_out"] * meta["outdoor_psf_pct"])

    bm = get_bed_matched_psf(
        all_sold, block, beds, comp_psf,
        lat=prop.get("lat"), lon=prop.get("lon"), is_zip=is_zip,
    )
    ref_psf = bm["psf"] or comp_psf
    adj_block_psf = (ref_psf / (1 + block_avg_prem / 100)) * (1 + combo_pct / 100)

    hoa = prop.get("hoa")
    sqft = prop.get("sqft")
    if hoa and sqft:
        hoa_adj_psf = psf - (hoa * 12 / HOA_CAP_RATE) / sqft
    else:
        hoa_adj_psf = psf

    price_pct = (adj_block_psf - hoa_adj_psf) / adj_block_psf * 100
    price_score = min(100, max(0, 50 + price_pct * 1.5))
    deal = round(bq * 0.60 + price_score * 0.40)

    return {
        "deal": deal,
        "price_pct": round(price_pct, 2),
        "adj_block_psf": round(adj_block_psf, 2),
        "hoa_adj_psf": round(hoa_adj_psf, 2),
        "ref_psf_source": bm["source"],
        "using_radius": not prop.get("block_psf") and bool(prop.get("radius_psf")),
    }


def load_all_sold(session: Session, city_key: str):
    """Return list of property-data dicts for all SOLD props in a city."""
    rows = (session.query(Property)
            .filter_by(city_key=city_key, status="SOLD")
            .all())
    return [r.data for r in rows]


def score_active_listings(session: Session, city_key: str):
    """Score every ACTIVE listing in a city. Returns list of
    {address, unit, list_price, beds, score: {...}} dicts."""
    actives = (session.query(Property)
               .filter_by(city_key=city_key, status="ACTIVE")
               .all())
    all_sold = load_all_sold(session, city_key)

    out = []
    for p in actives:
        data = dict(p.data)
        # If no psf set, derive from list_price / sqft like the dashboard does.
        if not data.get("psf") and data.get("list_price") and data.get("sqft"):
            data["psf"] = data["list_price"] / data["sqft"]
        score = compute_deal_score(data, all_sold, city_key)
        out.append({
            "mls": data.get("mls"),
            "address": data.get("address"),
            "unit": data.get("unit"),
            "list_price": data.get("list_price"),
            "beds": data.get("beds"),
            "baths": data.get("baths"),
            "sqft": data.get("sqft"),
            "psf": data.get("psf"),
            "block": data.get("block"),
            "listing_date": data.get("listing_date"),
            "score": score,
        })
    return out
