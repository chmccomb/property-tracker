"""SQLAlchemy engine + session factory.

SQLite for Phase 1; DATABASE_URL env var lets us swap to Postgres in Phase 2
without code changes.
"""

import os
from pathlib import Path

from sqlalchemy import create_engine
from sqlalchemy.orm import DeclarativeBase, sessionmaker

ROOT = Path(__file__).resolve().parent.parent
DEFAULT_SQLITE = f"sqlite:///{ROOT / 'data' / 'app.db'}"
DATABASE_URL = os.getenv("DATABASE_URL", DEFAULT_SQLITE)

_connect_args = {"check_same_thread": False} if DATABASE_URL.startswith("sqlite") else {}
engine = create_engine(DATABASE_URL, connect_args=_connect_args, future=True)
SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)


class Base(DeclarativeBase):
    pass


def get_session():
    s = SessionLocal()
    try:
        yield s
    finally:
        s.close()
