"""FastAPI app — read-only endpoints over the SQLite store.

Phase 1 goals: serve per-city dashboard payload from the DB instead of a
6MB baked-in HTML blob. Server-side deal scoring lives in app/scoring.py
(Phase 1 Day 2).
"""

from fastapi import Depends, FastAPI, HTTPException
from sqlalchemy.orm import Session

from app.db import engine, get_session
from app.models import Block, City, Property

app = FastAPI(title="Property Tracker", version="0.1.0")


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


@app.get("/cities/{key}/blocks")
def city_blocks(key: str, session: Session = Depends(get_session)):
    if not session.get(City, key):
        raise HTTPException(404, f"city '{key}' not found")
    blocks = (session.query(Block)
              .filter_by(city_key=key)
              .order_by(Block.emg_score.desc().nullslast())
              .all())
    return [b.data for b in blocks]
