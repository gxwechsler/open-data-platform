"""
Crisis-related SQLAlchemy ORM models.

These models store financial crisis data from sources like IMF Laeven-Valencia.
"""

from datetime import datetime
from decimal import Decimal
from typing import Optional

from sqlalchemy import (
    Boolean,
    ForeignKey,
    Index,
    Integer,
    Numeric,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship

from open_data.db.models import Base


class CrisisType(Base):
    """Types of financial crises."""

    __tablename__ = "crisis_types"

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    code: Mapped[str] = mapped_column(String(20), unique=True, nullable=False)
    name: Mapped[str] = mapped_column(String(100), nullable=False)
    description: Mapped[Optional[str]] = mapped_column(Text)

    # Relationships
    crises: Mapped[list["Crisis"]] = relationship(back_populates="crisis_type")

    def __repr__(self) -> str:
        return f"<CrisisType(code={self.code}, name={self.name})>"


class Crisis(Base):
    """
    Individual crisis events.
    
    Based on IMF Laeven-Valencia Systemic Banking Crises Database structure.
    """

    __tablename__ = "crises"
    __table_args__ = (
        UniqueConstraint("country_id", "crisis_type_id", "start_year", name="uq_crisis_country_type_year"),
        Index("idx_crisis_country", "country_id"),
        Index("idx_crisis_type", "crisis_type_id"),
        Index("idx_crisis_years", "start_year", "end_year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), nullable=False)
    crisis_type_id: Mapped[int] = mapped_column(ForeignKey("crisis_types.id"), nullable=False)
    source_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sources.id"))
    
    # Timing
    start_year: Mapped[int] = mapped_column(Integer, nullable=False)
    end_year: Mapped[Optional[int]] = mapped_column(Integer)
    peak_year: Mapped[Optional[int]] = mapped_column(Integer)
    duration_years: Mapped[Optional[int]] = mapped_column(Integer)
    
    # Severity metrics
    output_loss_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    fiscal_cost_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    fiscal_cost_net_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    monetary_expansion_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    
    # Banking crisis specific
    peak_npl_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))  # Non-performing loans
    liquidity_support_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    recapitalization_cost_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    asset_purchases_pct_gdp: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    
    # Currency crisis specific
    currency_depreciation_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    reserves_loss_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    
    # Sovereign debt crisis specific
    debt_restructured: Mapped[Optional[bool]] = mapped_column(Boolean)
    haircut_pct: Mapped[Optional[Decimal]] = mapped_column(Numeric(10, 2))
    
    # Policy responses (boolean flags)
    deposit_freeze: Mapped[Optional[bool]] = mapped_column(Boolean)
    bank_holiday: Mapped[Optional[bool]] = mapped_column(Boolean)
    blanket_guarantee: Mapped[Optional[bool]] = mapped_column(Boolean)
    nationalization: Mapped[Optional[bool]] = mapped_column(Boolean)
    bank_restructuring: Mapped[Optional[bool]] = mapped_column(Boolean)
    imf_program: Mapped[Optional[bool]] = mapped_column(Boolean)
    
    # Additional info
    notes: Mapped[Optional[str]] = mapped_column(Text)
    created_at: Mapped[datetime] = mapped_column(default=datetime.utcnow)
    updated_at: Mapped[Optional[datetime]] = mapped_column(onupdate=datetime.utcnow)

    # Relationships
    country: Mapped["Country"] = relationship("Country")
    crisis_type: Mapped["CrisisType"] = relationship(back_populates="crises")
    source: Mapped[Optional["Source"]] = relationship("Source")

    @property
    def is_ongoing(self) -> bool:
        """Check if crisis is still ongoing."""
        return self.end_year is None

    @property
    def calculated_duration(self) -> int | None:
        """Calculate duration if not stored."""
        if self.duration_years:
            return self.duration_years
        if self.end_year and self.start_year:
            return self.end_year - self.start_year + 1
        return None

    def __repr__(self) -> str:
        return f"<Crisis(country_id={self.country_id}, type={self.crisis_type_id}, start={self.start_year})>"


class CrisisIndicator(Base):
    """
    Time-series indicators related to crises.
    
    Stores annual data for crisis-related metrics like NPL ratios,
    credit growth, etc. that can be tracked over time.
    """

    __tablename__ = "crisis_indicators"
    __table_args__ = (
        UniqueConstraint("country_id", "indicator_code", "year", name="uq_crisis_indicator"),
        Index("idx_crisis_ind_country_year", "country_id", "year"),
    )

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    country_id: Mapped[int] = mapped_column(ForeignKey("countries.id"), nullable=False)
    indicator_code: Mapped[str] = mapped_column(String(50), nullable=False)
    indicator_name: Mapped[str] = mapped_column(String(200), nullable=False)
    year: Mapped[int] = mapped_column(Integer, nullable=False)
    value: Mapped[Optional[Decimal]] = mapped_column(Numeric(20, 4))
    source_id: Mapped[Optional[int]] = mapped_column(ForeignKey("sources.id"))

    # Relationships
    country: Mapped["Country"] = relationship("Country")
    source: Mapped[Optional["Source"]] = relationship("Source")

    def __repr__(self) -> str:
        return f"<CrisisIndicator(country={self.country_id}, code={self.indicator_code}, year={self.year})>"


# Import Country and Source for relationship resolution
from open_data.db.models import Country, Source
