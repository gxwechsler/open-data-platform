"""
SQLAlchemy ORM models for Open Data Platform.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    BigInteger,
    Boolean,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import DeclarativeBase, Mapped, mapped_column, relationship


class Base(DeclarativeBase):
    """Base class for all ORM models."""

    pass


class Country(Base):
    """Country dimension table."""

    __tablename__ = "countries"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    iso3_code: Mapped[str] = mapped_column(String(3), unique=True, nullable=False)
    iso2_code: Mapped[str] = mapped_column(String(2), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    region: Mapped[str] = mapped_column(String(50), nullable=False)
    subregion: Mapped[Optional[str]] = mapped_column(String(50))
    income_level: Mapped[Optional[str]] = mapped_column(String(50))
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    # Relationships
    observations: Mapped[list["Observation"]] = relationship(back_populates="country")

    def __repr__(self) -> str:
        return f"<Country(iso3={self.iso3_code}, name={self.name})>"


class Source(Base):
    """Data source reference table."""

    __tablename__ = "sources"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    base_url: Mapped[Optional[str]] = mapped_column(String(255))
    description: Mapped[Optional[str]] = mapped_column(Text)
    last_updated: Mapped[Optional[datetime]] = mapped_column()

    # Relationships
    indicators: Mapped[list["Indicator"]] = relationship(back_populates="source")
    ingestion_logs: Mapped[list["IngestionLog"]] = relationship(back_populates="source")

    def __repr__(self) -> str:
        return f"<Source(code={self.code}, name={self.name})>"


class Category(Base):
    """Indicator category (hierarchical)."""

    __tablename__ = "categories"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    parent_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    parent: Mapped[Optional["Category"]] = relationship(
        "Category", remote_side=[id], back_populates="children"
    )
    children: Mapped[list["Category"]] = relationship("Category", back_populates="parent")
    indicators: Mapped[list["Indicator"]] = relationship(back_populates="category")

    def __repr__(self) -> str:
        return f"<Category(code={self.code}, name={self.name})>"


class Indicator(Base):
    """Indicator definition."""

    __tablename__ = "indicators"
    __table_args__ = (UniqueConstraint("source_id", "code", name="uq_indicator_source_code"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sources.id"))
    category_id: Mapped[Optional[int]] = mapped_column(ForeignKey("categories.id"))
    code: Mapped[str] = mapped_column(String(100), nullable=False)
    name: Mapped[str] = mapped_column(String(255), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    unit: Mapped[Optional[str]] = mapped_column(String(100))
    frequency: Mapped[str] = mapped_column(String(20), default="annual")

    # Relationships
    source: Mapped[Optional["Source"]] = relationship(back_populates="indicators")
    category: Mapped[Optional["Category"]] = relationship(back_populates="indicators")
    observations: Mapped[list["Observation"]] = relationship(back_populates="indicator")
    group_memberships: Mapped[list["IndicatorGroupMember"]] = relationship(
        back_populates="indicator"
    )

    def __repr__(self) -> str:
        return f"<Indicator(code={self.code}, name={self.name[:30]})>"


class Observation(Base):
    """
    Main fact table for time series data.

    Note: This table is partitioned by year in PostgreSQL.
    The partitioning is handled by the init.sql script.
    """

    __tablename__ = "observations"
    __table_args__ = (
        Index("idx_obs_country_year", "country_id", "year"),
        Index("idx_obs_indicator_year", "indicator_id", "year"),
        Index("idx_obs_country_indicator", "country_id", "indicator_id"),
        {"postgresql_partition_by": "RANGE (year)"},
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), nullable=False)
    indicator_id: Mapped[int] = mapped_column(ForeignKey("indicators.id"), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False, primary_key=True)
    value: Mapped[Optional[Decimal]] = mapped_column(Numeric)
    is_estimated: Mapped[bool] = mapped_column(Boolean, default=False)
    source_note: Mapped[Optional[str]] = mapped_column(Text)
    fetched_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    # Relationships
    country: Mapped["Country"] = relationship(back_populates="observations")
    indicator: Mapped["Indicator"] = relationship(back_populates="observations")

    def __repr__(self) -> str:
        return f"<Observation(country={self.country_id}, indicator={self.indicator_id}, year={self.year}, value={self.value})>"


class IngestionLog(Base):
    """Track data ingestion jobs."""

    __tablename__ = "ingestion_logs"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    source_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sources.id"))
    started_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    completed_at: Mapped[Optional[datetime]] = mapped_column()
    status: Mapped[str] = mapped_column(String(20), default="running")
    records_processed: Mapped[int] = mapped_column(Integer, default=0)
    error_message: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    source: Mapped[Optional["Source"]] = relationship(back_populates="ingestion_logs")

    def __repr__(self) -> str:
        return f"<IngestionLog(id={self.id}, source={self.source_id}, status={self.status})>"


class IndicatorGroup(Base):
    """User-defined indicator groups."""

    __tablename__ = "indicator_groups"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)

    # Relationships
    members: Mapped[list["IndicatorGroupMember"]] = relationship(back_populates="group")

    def __repr__(self) -> str:
        return f"<IndicatorGroup(id={self.id}, name={self.name})>"


class IndicatorGroupMember(Base):
    """Indicator group membership (many-to-many)."""

    __tablename__ = "indicator_group_members"

    group_id: Mapped[int] = mapped_column(
        ForeignKey("indicator_groups.id", ondelete="CASCADE"), primary_key=True
    )
    indicator_id: Mapped[int] = mapped_column(
        ForeignKey("indicators.id", ondelete="CASCADE"), primary_key=True
    )

    # Relationships
    group: Mapped["IndicatorGroup"] = relationship(back_populates="members")
    indicator: Mapped["Indicator"] = relationship(back_populates="group_memberships")

    def __repr__(self) -> str:
        return f"<IndicatorGroupMember(group={self.group_id}, indicator={self.indicator_id})>"
