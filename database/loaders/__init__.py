"""
Database Loaders Package.

Data loaders for populating the PostgreSQL database.

Available Loaders:
    - FREDLoader: Federal Reserve Economic Data
    - DisasterLoader: EM-DAT disaster database
    - CrisisLoader: Financial crisis data (Reinhart-Rogoff, Laeven-Valencia)

Usage:
    from database.loaders import FREDLoader, DisasterLoader, CrisisLoader
    
    # Load sample data (no API key required)
    fred = FREDLoader()
    fred.load_sample_data()
    
    # Load from FRED API (requires FRED_API_KEY)
    fred.load_series("FEDFUNDS", start_date="2000-01-01")
    
    # Load disasters
    disasters = DisasterLoader()
    disasters.load_sample_data()
    
    # Load crises
    crises = CrisisLoader()
    crises.load_sample_data()

Command Line:
    python -m database.loaders.fred_loader --sample
    python -m database.loaders.disaster_loader --sample
    python -m database.loaders.crisis_loader --sample
"""

from .fred_loader import FREDLoader
from .disaster_loader import DisasterLoader
from .crisis_loader import CrisisLoader

__all__ = [
    "FREDLoader",
    "DisasterLoader", 
    "CrisisLoader",
]
