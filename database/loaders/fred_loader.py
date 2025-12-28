"""
FRED Data Loader.

Loads Federal Reserve Economic Data into PostgreSQL.

Supports:
- Live API fetching (requires FRED_API_KEY)
- Sample data loading (no API key needed)

Usage:
    python -m database.loaders.fred_loader --sample
    python -m database.loaders.fred_loader --series FEDFUNDS DGS10
    python -m database.loaders.fred_loader --all
"""

import argparse
import os
from datetime import datetime, date
from decimal import Decimal
from typing import Optional

import pandas as pd

# Try to import fredapi
try:
    from fredapi import Fred
    HAS_FREDAPI = True
except ImportError:
    HAS_FREDAPI = False

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from database.connection import DatabaseManager
from database.models import FedSeries, FedSeriesMeta


# =============================================================================
# SERIES DEFINITIONS
# =============================================================================

FRED_SERIES = {
    # Interest Rates
    "FEDFUNDS": ("Federal Funds Effective Rate", "Interest Rates", "Percent", "Monthly"),
    "DGS10": ("10-Year Treasury Yield", "Interest Rates", "Percent", "Daily"),
    "DGS2": ("2-Year Treasury Yield", "Interest Rates", "Percent", "Daily"),
    "DGS30": ("30-Year Treasury Yield", "Interest Rates", "Percent", "Daily"),
    "DGS3MO": ("3-Month Treasury Yield", "Interest Rates", "Percent", "Daily"),
    "T10Y2Y": ("10Y-2Y Treasury Spread", "Interest Rates", "Percent", "Daily"),
    "T10Y3M": ("10Y-3M Treasury Spread", "Interest Rates", "Percent", "Daily"),
    "DPRIME": ("Bank Prime Loan Rate", "Interest Rates", "Percent", "Daily"),
    "MORTGAGE30US": ("30-Year Mortgage Rate", "Interest Rates", "Percent", "Weekly"),
    
    # Inflation
    "CPIAUCSL": ("Consumer Price Index (All Urban)", "Inflation", "Index", "Monthly"),
    "CPILFESL": ("Core CPI (Less Food & Energy)", "Inflation", "Index", "Monthly"),
    "PCEPI": ("PCE Price Index", "Inflation", "Index", "Monthly"),
    "PCEPILFE": ("Core PCE Price Index", "Inflation", "Index", "Monthly"),
    "T5YIE": ("5-Year Breakeven Inflation", "Inflation", "Percent", "Daily"),
    "T10YIE": ("10-Year Breakeven Inflation", "Inflation", "Percent", "Daily"),
    
    # Fed Balance Sheet
    "WALCL": ("Fed Total Assets", "Fed Balance Sheet", "Millions USD", "Weekly"),
    "WTREGEN": ("Fed Treasury Holdings", "Fed Balance Sheet", "Millions USD", "Weekly"),
    "WSHOMCB": ("Fed MBS Holdings", "Fed Balance Sheet", "Millions USD", "Weekly"),
    "RRPONTSYD": ("Reverse Repo (ON RRP)", "Fed Balance Sheet", "Billions USD", "Daily"),
    
    # Money Supply
    "M2SL": ("M2 Money Stock", "Money Supply", "Billions USD", "Monthly"),
    "M1SL": ("M1 Money Stock", "Money Supply", "Billions USD", "Monthly"),
    "BOGMBASE": ("Monetary Base", "Money Supply", "Billions USD", "Monthly"),
    
    # Economic Activity
    "UNRATE": ("Unemployment Rate", "Labor", "Percent", "Monthly"),
    "PAYEMS": ("Nonfarm Payrolls", "Labor", "Thousands", "Monthly"),
    "ICSA": ("Initial Jobless Claims", "Labor", "Number", "Weekly"),
    "GDP": ("Nominal GDP", "Output", "Billions USD", "Quarterly"),
    "GDPC1": ("Real GDP", "Output", "Billions 2017 USD", "Quarterly"),
    
    # Markets
    "VIXCLS": ("VIX Volatility Index", "Markets", "Index", "Daily"),
    "SP500": ("S&P 500 Index", "Markets", "Index", "Daily"),
    "BAMLH0A0HYM2": ("High Yield Spread", "Markets", "Percent", "Daily"),
    "DTWEXBGS": ("Trade Weighted Dollar Index", "Markets", "Index", "Daily"),
    
    # Financial Stress
    "NFCI": ("Financial Conditions Index", "Stress", "Index", "Weekly"),
    "STLFSI4": ("Financial Stress Index", "Stress", "Index", "Weekly"),
}


# =============================================================================
# SAMPLE DATA
# =============================================================================

def generate_sample_data() -> dict:
    """Generate sample FRED data for demo purposes."""
    import numpy as np
    np.random.seed(42)
    
    dates_monthly = pd.date_range("2000-01-01", "2024-12-01", freq="MS")
    dates_daily = pd.date_range("2020-01-01", "2024-12-31", freq="B")  # Business days
    
    data = {}
    
    # Fed Funds Rate (monthly)
    ff = []
    rate = 6.5
    for d in dates_monthly:
        year = d.year
        if year <= 2001: rate = max(1.0, rate - 0.15)
        elif year <= 2004: rate = max(1.0, rate - 0.02)
        elif year <= 2006: rate = min(5.25, rate + 0.08)
        elif year <= 2008: rate = max(0.1, rate - 0.25)
        elif year <= 2015: rate = 0.1
        elif year <= 2018: rate = min(2.5, rate + 0.05)
        elif year <= 2019: rate = max(1.5, rate - 0.03)
        elif year <= 2021: rate = 0.08
        elif year <= 2023: rate = min(5.5, rate + 0.15)
        else: rate = max(4.5, rate - 0.05)
        ff.append(rate)
    data["FEDFUNDS"] = pd.Series(ff, index=dates_monthly)
    
    # 10-Year Treasury (monthly)
    dgs10 = [ff[i] + 1.5 + np.random.normal(0, 0.3) for i in range(len(ff))]
    data["DGS10"] = pd.Series(dgs10, index=dates_monthly)
    
    # 2-Year Treasury (monthly)
    dgs2 = [ff[i] + 0.5 + np.random.normal(0, 0.2) for i in range(len(ff))]
    data["DGS2"] = pd.Series(dgs2, index=dates_monthly)
    
    # Yield spread
    data["T10Y2Y"] = data["DGS10"] - data["DGS2"]
    
    # CPI
    cpi = [170]
    for d in dates_monthly[1:]:
        year = d.year
        if year <= 2020: growth = 1.002
        elif year <= 2022: growth = 1.005
        else: growth = 1.0025
        cpi.append(cpi[-1] * growth)
    data["CPIAUCSL"] = pd.Series(cpi, index=dates_monthly)
    
    # Fed Assets (weekly approximated as monthly)
    walcl = []
    assets = 0.7
    for d in dates_monthly:
        year = d.year
        if year < 2008: assets = 0.7 + 0.02 * (year - 2000)
        elif year <= 2014: assets = min(4.5, assets + 0.12)
        elif year <= 2017: assets = max(4.0, assets - 0.02)
        elif year <= 2019: assets = max(3.8, assets - 0.03)
        elif year <= 2022: assets = min(9.0, assets + 0.15)
        else: assets = max(7.0, assets - 0.05)
        walcl.append(assets * 1e6)  # Millions
    data["WALCL"] = pd.Series(walcl, index=dates_monthly)
    
    # M2
    m2 = [4600]
    for d in dates_monthly[1:]:
        year = d.year
        if year < 2020: growth = 1.005
        elif year <= 2022: growth = 1.015
        else: growth = 0.998
        m2.append(m2[-1] * growth)
    data["M2SL"] = pd.Series(m2, index=dates_monthly)
    
    # Unemployment
    ur = []
    rate = 4.0
    for d in dates_monthly:
        year, month = d.year, d.month
        if year <= 2003: rate = min(6.5, rate + 0.05)
        elif year <= 2007: rate = max(4.5, rate - 0.04)
        elif year <= 2009: rate = min(10.0, rate + 0.2)
        elif year <= 2019: rate = max(3.5, rate - 0.03)
        elif year == 2020 and month <= 6: rate = min(14.0, rate + 2.0)
        elif year <= 2022: rate = max(3.5, rate - 0.15)
        else: rate = min(4.5, rate + 0.02)
        ur.append(rate)
    data["UNRATE"] = pd.Series(ur, index=dates_monthly)
    
    # VIX (daily, recent only)
    vix = []
    for d in dates_daily:
        base = 18
        if d.year == 2020 and d.month in [3, 4]: base = 50
        elif d.year == 2022 and d.month in [2, 3]: base = 30
        vix.append(max(10, min(80, base + np.random.normal(0, 5))))
    data["VIXCLS"] = pd.Series(vix, index=dates_daily)
    
    return data


# =============================================================================
# LOADER CLASS
# =============================================================================

class FREDLoader:
    """Load FRED data into PostgreSQL."""
    
    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or os.environ.get("FRED_API_KEY")
        self.fred = None
        self.db = DatabaseManager()
        
        if self.api_key and HAS_FREDAPI:
            try:
                self.fred = Fred(api_key=self.api_key)
            except Exception as e:
                print(f"Warning: Could not initialize FRED API: {e}")
    
    @property
    def can_fetch_live(self) -> bool:
        return self.fred is not None
    
    def load_series_metadata(self) -> int:
        """Load series metadata into database."""
        count = 0
        
        with self.db.session() as session:
            for series_id, (name, category, units, frequency) in FRED_SERIES.items():
                existing = session.query(FedSeriesMeta).filter_by(series_id=series_id).first()
                
                if existing:
                    continue
                
                meta = FedSeriesMeta(
                    series_id=series_id,
                    name=name,
                    category=category,
                    units=units,
                    frequency=frequency,
                )
                session.add(meta)
                count += 1
        
        return count
    
    def load_sample_data(self) -> dict:
        """Load sample data (no API key required)."""
        sample = generate_sample_data()
        results = {}
        
        for series_id, data in sample.items():
            count = self._insert_series(series_id, data)
            results[series_id] = count
            print(f"  {series_id}: {count} observations")
        
        return results
    
    def fetch_series(
        self,
        series_id: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
    ) -> pd.Series:
        """Fetch series from FRED API."""
        if not self.can_fetch_live:
            raise ValueError("FRED API not available. Set FRED_API_KEY environment variable.")
        
        return self.fred.get_series(
            series_id,
            observation_start=start_date,
            observation_end=end_date,
        )
    
    def load_series(
        self,
        series_id: str,
        start_date: Optional[str] = None,
    ) -> int:
        """Fetch and load a single series."""
        data = self.fetch_series(series_id, start_date)
        return self._insert_series(series_id, data)
    
    def load_all_series(self, start_date: str = "2000-01-01") -> dict:
        """Fetch and load all defined series."""
        results = {}
        
        for series_id in FRED_SERIES:
            try:
                count = self.load_series(series_id, start_date)
                results[series_id] = count
                print(f"  {series_id}: {count} observations")
            except Exception as e:
                results[series_id] = f"ERROR: {e}"
                print(f"  {series_id}: ERROR - {e}")
        
        return results
    
    def _insert_series(self, series_id: str, data: pd.Series) -> int:
        """Insert series data into database."""
        if data is None or len(data) == 0:
            return 0
        
        meta = FRED_SERIES.get(series_id, (series_id, None, None, None))
        name, category, units, frequency = meta
        
        count = 0
        
        with self.db.session() as session:
            for dt, value in data.items():
                if pd.isna(value):
                    continue
                
                # Convert to date
                if isinstance(dt, pd.Timestamp):
                    obs_date = dt.date()
                else:
                    obs_date = dt
                
                # Check if exists
                existing = session.query(FedSeries).filter_by(
                    series_id=series_id,
                    date=obs_date,
                ).first()
                
                if existing:
                    existing.value = Decimal(str(value))
                else:
                    record = FedSeries(
                        series_id=series_id,
                        date=obs_date,
                        value=Decimal(str(value)),
                        country_iso3="USA",
                        series_name=name,
                        units=units,
                        frequency=frequency,
                    )
                    session.add(record)
                    count += 1
        
        return count
    
    def get_stats(self) -> dict:
        """Get loading statistics."""
        with self.db.session() as session:
            from sqlalchemy import func, text
            
            total = session.query(FedSeries).count()
            
            series_counts = session.execute(text("""
                SELECT series_id, COUNT(*) as cnt, MIN(date) as min_date, MAX(date) as max_date
                FROM fed_series
                GROUP BY series_id
                ORDER BY series_id
            """))
            
            by_series = {row[0]: {"count": row[1], "start": row[2], "end": row[3]} 
                        for row in series_counts}
        
        return {
            "total_observations": total,
            "series_count": len(by_series),
            "by_series": by_series,
        }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="FRED Data Loader")
    parser.add_argument("--sample", action="store_true", help="Load sample data (no API key)")
    parser.add_argument("--series", nargs="+", help="Load specific series")
    parser.add_argument("--all", action="store_true", help="Load all series (requires API key)")
    parser.add_argument("--metadata", action="store_true", help="Load series metadata only")
    parser.add_argument("--stats", action="store_true", help="Show database statistics")
    parser.add_argument("--start-date", default="2000-01-01", help="Start date for data")
    
    args = parser.parse_args()
    
    loader = FREDLoader()
    
    print("=" * 60)
    print("FRED Data Loader")
    print("=" * 60)
    print(f"\nAPI Available: {loader.can_fetch_live}")
    print(f"Database Connected: {loader.db.test_connection()}")
    
    if args.metadata:
        print("\nLoading series metadata...")
        count = loader.load_series_metadata()
        print(f"✅ Loaded {count} series definitions")
    
    if args.sample:
        print("\nLoading sample data...")
        loader.load_series_metadata()
        results = loader.load_sample_data()
        print(f"✅ Loaded {sum(v for v in results.values() if isinstance(v, int))} total observations")
    
    if args.series:
        print(f"\nLoading series: {args.series}")
        for series_id in args.series:
            try:
                count = loader.load_series(series_id, args.start_date)
                print(f"  {series_id}: {count} observations")
            except Exception as e:
                print(f"  {series_id}: ERROR - {e}")
    
    if args.all:
        print(f"\nLoading all series from {args.start_date}...")
        loader.load_series_metadata()
        results = loader.load_all_series(args.start_date)
    
    if args.stats:
        print("\n📊 Database Statistics:")
        stats = loader.get_stats()
        print(f"  Total observations: {stats['total_observations']:,}")
        print(f"  Series count: {stats['series_count']}")
        if stats['by_series']:
            print("\n  By series:")
            for sid, info in list(stats['by_series'].items())[:10]:
                print(f"    {sid}: {info['count']:,} obs ({info['start']} to {info['end']})")
    
    if not any([args.sample, args.series, args.all, args.metadata, args.stats]):
        parser.print_help()


if __name__ == "__main__":
    main()
