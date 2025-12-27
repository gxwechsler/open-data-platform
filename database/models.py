"""SQLAlchemy models for Open Data Platform."""
from datetime import datetime
from sqlalchemy import Column, Integer, BigInteger, String, Text, Float, Boolean, Date, DateTime, Numeric, Index
from sqlalchemy.orm import declarative_base

Base = declarative_base()

class FedSeriesMeta(Base):
    """Metadata for Federal Reserve economic data series."""
    __tablename__ = 'fed_series_meta'
    
    series_id = Column(String(50), primary_key=True)
    name = Column(String(200), nullable=False)
    description = Column(Text)
    category = Column(String(100))
    units = Column(String(100))
    frequency = Column(String(20))
    seasonal_adjustment = Column(String(50))
    last_updated = Column(DateTime)
    observation_start = Column(Date)
    observation_end = Column(Date)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class FedSeries(Base):
    """Federal Reserve economic data series observations."""
    __tablename__ = 'fed_series'
    
    id = Column(BigInteger, primary_key=True, autoincrement=True)
    series_id = Column(String(50), nullable=False)
    date = Column(Date, nullable=False)
    value = Column(Numeric(20, 6))
    country_iso3 = Column(String(3), default='USA')
    series_name = Column(String(200))
    units = Column(String(100))
    frequency = Column(String(20))
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_fed_series_date', 'series_id', 'date'),
    )

class Disaster(Base):
    """Natural disaster events from EM-DAT."""
    __tablename__ = 'disasters'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    emdat_id = Column(String(20), unique=True)
    country_iso3 = Column(String(3), nullable=False)
    disaster_type = Column(String(50), nullable=False)
    disaster_group = Column(String(50))
    disaster_subtype = Column(String(100))
    event_name = Column(String(200))
    start_date = Column(Date)
    end_date = Column(Date)
    year = Column(Integer, nullable=False)
    location = Column(Text)
    latitude = Column(Float)
    longitude = Column(Float)
    magnitude = Column(Float)
    magnitude_scale = Column(String(20))
    deaths = Column(Integer)
    deaths_missing = Column(Integer)
    injured = Column(Integer)
    affected = Column(BigInteger)
    homeless = Column(Integer)
    total_affected = Column(BigInteger)
    damage_usd = Column(Numeric(20, 2))
    damage_adjusted = Column(Numeric(20, 2))
    insured_damage_usd = Column(Numeric(20, 2))
    notes = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_disasters_type_year', 'disaster_type', 'year'),
        Index('ix_disasters_country_year', 'country_iso3', 'year'),
    )

class FinancialCrisis(Base):
    """Financial crisis events from Reinhart-Rogoff and Laeven-Valencia."""
    __tablename__ = 'financial_crises'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    country_iso3 = Column(String(3), nullable=False)
    crisis_type = Column(String(50), nullable=False)
    start_year = Column(Integer, nullable=False)
    end_year = Column(Integer)
    peak_year = Column(Integer)
    source = Column(String(50), nullable=False)
    source_notes = Column(Text)
    output_loss_pct = Column(Float)
    fiscal_cost_pct = Column(Float)
    monetary_expansion_pct = Column(Float)
    peak_npl_pct = Column(Float)
    exchange_rate_depreciation = Column(Float)
    peak_inflation = Column(Float)
    peak_month = Column(String(20))
    haircut_pct = Column(Float)
    external_default = Column(Boolean)
    domestic_default = Column(Boolean)
    duration_years = Column(Float)
    resolution_type = Column(String(100))
    description = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    __table_args__ = (
        Index('ix_crises_type_year', 'crisis_type', 'start_year'),
        Index('ix_crises_country_year', 'country_iso3', 'start_year'),
    )
