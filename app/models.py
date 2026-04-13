"""ORM models for cities, blocks, and properties.

JSON columns hold the per-row payload verbatim so the API can return
dashboard-ready dicts without re-mapping every field. Promoted columns
exist for fields we filter/sort/score on.
"""

from typing import List, Optional

from sqlalchemy import (
    Boolean, Float, ForeignKey, Index, Integer, JSON, String, UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from app.db import Base


class City(Base):
    __tablename__ = "cities"

    key: Mapped[str] = mapped_column(String(32), primary_key=True)
    name: Mapped[str] = mapped_column(String(64), nullable=False)
    default_zip: Mapped[str] = mapped_column(String(10), nullable=False)
    radius_miles: Mapped[float] = mapped_column(Float, nullable=False)
    overall_trend: Mapped[dict] = mapped_column(JSON, default=dict)

    blocks: Mapped[List["Block"]] = relationship(back_populates="city",
                                                 cascade="all, delete-orphan")
    properties: Mapped[List["Property"]] = relationship(back_populates="city",
                                                       cascade="all, delete-orphan")


class Block(Base):
    __tablename__ = "blocks"

    id: Mapped[int] = mapped_column(primary_key=True)
    city_key: Mapped[str] = mapped_column(ForeignKey("cities.key"), nullable=False)
    block: Mapped[str] = mapped_column(String(64), nullable=False)
    est_score: Mapped[Optional[float]] = mapped_column(Float)
    emg_score: Mapped[Optional[float]] = mapped_column(Float)
    n_sales: Mapped[Optional[float]] = mapped_column(Float)
    med_psf: Mapped[Optional[float]] = mapped_column(Float)
    cagr: Mapped[Optional[float]] = mapped_column(Float)
    trend: Mapped[dict] = mapped_column(JSON, default=dict)
    data: Mapped[dict] = mapped_column(JSON, nullable=False)

    city: Mapped["City"] = relationship(back_populates="blocks")

    __table_args__ = (
        UniqueConstraint("city_key", "block", name="uq_block_city_block"),
        Index("ix_blocks_city_emg", "city_key", "emg_score"),
        Index("ix_blocks_city_est", "city_key", "est_score"),
    )


class Property(Base):
    __tablename__ = "properties"

    id: Mapped[int] = mapped_column(primary_key=True)
    city_key: Mapped[str] = mapped_column(ForeignKey("cities.key"), nullable=False)
    mls: Mapped[Optional[str]] = mapped_column(String(32), index=True)
    address: Mapped[Optional[str]] = mapped_column(String(255))
    unit: Mapped[Optional[str]] = mapped_column(String(32))
    block: Mapped[Optional[str]] = mapped_column(String(64), index=True)
    status: Mapped[Optional[str]] = mapped_column(String(16), index=True)
    beds: Mapped[Optional[float]] = mapped_column(Float)
    baths: Mapped[Optional[float]] = mapped_column(Float)
    sqft: Mapped[Optional[float]] = mapped_column(Float)
    sold_price: Mapped[Optional[float]] = mapped_column(Float)
    list_price: Mapped[Optional[float]] = mapped_column(Float)
    psf: Mapped[Optional[float]] = mapped_column(Float)
    closing_date: Mapped[Optional[str]] = mapped_column(String(16), index=True)
    lat: Mapped[Optional[float]] = mapped_column(Float)
    lon: Mapped[Optional[float]] = mapped_column(Float)
    parking: Mapped[bool] = mapped_column(Boolean, default=False)
    outdoor: Mapped[bool] = mapped_column(Boolean, default=False)
    distressed: Mapped[bool] = mapped_column(Boolean, default=False)
    data: Mapped[dict] = mapped_column(JSON, nullable=False)

    city: Mapped["City"] = relationship(back_populates="properties")

    __table_args__ = (
        Index("ix_properties_city_status", "city_key", "status"),
        Index("ix_properties_city_block", "city_key", "block"),
    )
