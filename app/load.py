"""Load per-city dashboard JSON (from pipeline.emerging) into SQLite.

Usage:
    python -m app.load            # load all cities in CITY_CONFIGS
    python -m app.load --city hoboken
"""

import argparse
import json
import sys
from pathlib import Path

from app.db import Base, engine, SessionLocal
from app.models import Block, City, Property
from pipeline.config import CITY_CONFIGS
from pipeline.emerging import _default_data_dir


def _json_path(city_key):
    cfg = CITY_CONFIGS[city_key]
    return _default_data_dir(city_key) / f"{cfg['output_prefix']}_dashboard_data_v2.json"


def load_city(city_key, session):
    cfg = CITY_CONFIGS[city_key]
    path = _json_path(city_key)
    if not path.exists():
        print(f"  SKIP {city_key}: {path} not found")
        return
    payload = json.loads(path.read_text())

    session.query(Property).filter_by(city_key=city_key).delete()
    session.query(Block).filter_by(city_key=city_key).delete()
    session.query(City).filter_by(key=city_key).delete()
    session.flush()

    session.add(City(
        key=city_key,
        name=cfg["name"],
        default_zip=cfg["default_zip"],
        radius_miles=cfg["radius_miles"],
        overall_trend=payload.get("overall_trend", {}),
    ))

    trends = payload.get("trends", {})
    for b in payload.get("blocks", []):
        session.add(Block(
            city_key=city_key,
            block=b.get("block", ""),
            est_score=b.get("est_score"),
            emg_score=b.get("emg_score"),
            n_sales=b.get("n_sales"),
            med_psf=b.get("med_psf"),
            cagr=b.get("cagr"),
            trend=trends.get(b.get("block", ""), {}),
            data=b,
        ))

    for p in payload.get("properties", []):
        session.add(Property(
            city_key=city_key,
            mls=p.get("mls") or None,
            address=p.get("address") or None,
            unit=p.get("unit") or None,
            block=p.get("block") or None,
            status=p.get("status") or None,
            beds=p.get("beds"),
            baths=p.get("baths"),
            sqft=p.get("sqft"),
            sold_price=p.get("sold_price"),
            list_price=p.get("list_price"),
            psf=p.get("psf"),
            closing_date=p.get("closing_date") or None,
            listing_date=p.get("listing_date") or None,
            lat=p.get("lat"),
            lon=p.get("lon"),
            parking=bool(p.get("parking")),
            outdoor=bool(p.get("outdoor")),
            distressed=bool(p.get("distressed")),
            data=p,
        ))

    session.commit()
    n_p = session.query(Property).filter_by(city_key=city_key).count()
    n_b = session.query(Block).filter_by(city_key=city_key).count()
    print(f"  {city_key}: {n_p} properties, {n_b} blocks loaded")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--city", default="all")
    args = ap.parse_args()

    Path(engine.url.database).parent.mkdir(parents=True, exist_ok=True)
    Base.metadata.create_all(engine)

    targets = list(CITY_CONFIGS) if args.city == "all" else [args.city]
    with SessionLocal() as session:
        for key in targets:
            if key not in CITY_CONFIGS:
                print(f"ERROR: unknown city '{key}'", file=sys.stderr)
                sys.exit(1)
            load_city(key, session)

    print(f"DB: {engine.url}")


if __name__ == "__main__":
    main()
