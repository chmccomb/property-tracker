"""FastAPI app — read-only endpoints over the SQLite store.

Phase 1 goals: serve per-city dashboard payload from the DB instead of a
6MB baked-in HTML blob. Server-side deal scoring lives in app/scoring.py
(Phase 1 Day 2).
"""

from pathlib import Path

from fastapi import Depends, FastAPI, HTTPException
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel
from sqlalchemy.orm import Session
from typing import Optional

STATIC_DIR = Path(__file__).resolve().parent / "static"

from app.db import engine, get_session
from app.models import Block, City, Property
from app.scoring import compute_deal_score, load_all_sold, score_active_listings


class ScoreRequest(BaseModel):
    psf: Optional[float] = None
    list_price: Optional[float] = None
    sqft: Optional[float] = None
    beds: Optional[float] = None
    baths: Optional[float] = None
    hoa: Optional[float] = None
    block: Optional[str] = None
    block_psf: Optional[float] = None
    radius_psf: Optional[float] = None
    est_score: Optional[float] = None
    emg_score: Optional[float] = None
    block_n: Optional[float] = None
    lat: Optional[float] = None
    lon: Optional[float] = None
    parking: bool = False
    outdoor: bool = False

app = FastAPI(title="Property Tracker", version="0.1.0")
app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/", include_in_schema=False)
def root():
    return FileResponse(STATIC_DIR / "dashboard.html")


@app.get("/health")
def health():
    return {"ok": True, "db": str(engine.url)}


@app.get("/cities")
def list_cities(session: Session = Depends(get_session)):
    return [
        {"key": c.key, "name": c.name, "default_zip": c.default_zip,
         "radius_miles": c.radius_miles}
        for c in session.query(City).order_by(City.key).all()
    ]


@app.get("/cities/{key}")
def city_dashboard(key: str, session: Session = Depends(get_session)):
    city = session.get(City, key)
    if not city:
        raise HTTPException(404, f"city '{key}' not found")
    blocks = session.query(Block).filter_by(city_key=key).all()
    props = session.query(Property).filter_by(city_key=key).all()
    trends = {b.block: b.trend for b in blocks if b.trend}
    return {
        "city": key,
        "city_name": city.name,
        "overall_trend": city.overall_trend,
        "trends": trends,
        "blocks": [b.data for b in blocks],
        "properties": [p.data for p in props],
    }


@app.get("/api/all")
def api_all(session: Session = Depends(get_session)):
    """Dashboard-shaped payload: {jc, hoboken, weehawken} each with
    properties / blocks / overall_trend. Key `jc` maps to city_key
    `jc_heights` to match the frontend's existing MARKETS structure."""
    key_to_market = {"jc_heights": "jc", "hoboken": "hoboken", "weehawken": "weehawken"}
    out = {}
    for city in session.query(City).all():
        market_key = key_to_market.get(city.key, city.key)
        blocks = session.query(Block).filter_by(city_key=city.key).all()
        props = session.query(Property).filter_by(city_key=city.key).all()
        out[market_key] = {
            "properties": [p.data for p in props],
            "blocks": [b.data for b in blocks],
            "overall_trend": city.overall_trend,
            "trends": {b.block: b.trend for b in blocks if b.trend},
        }
    return out


@app.get("/cities/{key}/actives")
def city_actives(key: str, session: Session = Depends(get_session)):
    """Scored active listings — deal score computed server-side."""
    if not session.get(City, key):
        raise HTTPException(404, f"city '{key}' not found")
    return score_active_listings(session, key)


@app.post("/cities/{key}/score")
def score_property(key: str, body: ScoreRequest,
                   session: Session = Depends(get_session)):
    """Ad-hoc deal score for a user-supplied property (Scorer card)."""
    if not session.get(City, key):
        raise HTTPException(404, f"city '{key}' not found")
    prop = body.model_dump()
    if not prop.get("psf") and prop.get("list_price") and prop.get("sqft"):
        prop["psf"] = prop["list_price"] / prop["sqft"]
    # If caller didn't pass block stats, try to fill from DB.
    if prop.get("block") and (prop.get("block_psf") is None
                              or prop.get("est_score") is None):
        b = (session.query(Block)
             .filter_by(city_key=key, block=prop["block"]).one_or_none())
        if b:
            if prop.get("est_score") is None: prop["est_score"] = b.est_score
            if prop.get("emg_score") is None: prop["emg_score"] = b.emg_score
            if prop.get("block_n")   is None: prop["block_n"]   = b.n_sales
            if prop.get("block_psf") is None: prop["block_psf"] = b.med_psf
    all_sold = load_all_sold(session, key)
    result = compute_deal_score(prop, all_sold, key)
    if result is None:
        raise HTTPException(422, "not enough data to score "
                                 "(need psf and block_psf or radius_psf)")
    return result


@app.get("/cities/{key}/blocks")
def city_blocks(key: str, session: Session = Depends(get_session)):
    if not session.get(City, key):
        raise HTTPException(404, f"city '{key}' not found")
    blocks = (session.query(Block)
              .filter_by(city_key=key)
              .order_by(Block.emg_score.desc().nullslast())
              .all())
    return [b.data for b in blocks]
