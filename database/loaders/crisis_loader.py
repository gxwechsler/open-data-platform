"""
Financial Crisis Data Loader.

Loads Reinhart-Rogoff and Laeven-Valencia crisis data into PostgreSQL.

Usage:
    python -m database.loaders.crisis_loader --sample
    python -m database.loaders.crisis_loader --stats
"""

import argparse
import os
from decimal import Decimal
from typing import Optional

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from database.connection import DatabaseManager
from database.models import FinancialCrisis, CrisisType, DataSource


# =============================================================================
# SAMPLE DATA - REINHART-ROGOFF
# =============================================================================

REINHART_ROGOFF_CRISES = [
    # External Defaults - Medieval/Early Modern
    {"country": "GBR", "type": CrisisType.SOVEREIGN, "start": 1340, "source": DataSource.REINHART_ROGOFF,
     "notes": "Edward III default to Italian bankers (Bardi, Peruzzi)"},
    {"country": "ESP", "type": CrisisType.SOVEREIGN, "start": 1557, "source": DataSource.REINHART_ROGOFF,
     "notes": "Philip II - first of 7 Spanish defaults"},
    {"country": "ESP", "type": CrisisType.SOVEREIGN, "start": 1575, "source": DataSource.REINHART_ROGOFF},
    {"country": "ESP", "type": CrisisType.SOVEREIGN, "start": 1596, "source": DataSource.REINHART_ROGOFF},
    {"country": "ESP", "type": CrisisType.SOVEREIGN, "start": 1607, "source": DataSource.REINHART_ROGOFF},
    {"country": "ESP", "type": CrisisType.SOVEREIGN, "start": 1627, "source": DataSource.REINHART_ROGOFF},
    {"country": "ESP", "type": CrisisType.SOVEREIGN, "start": 1647, "source": DataSource.REINHART_ROGOFF},
    {"country": "FRA", "type": CrisisType.SOVEREIGN, "start": 1788, "source": DataSource.REINHART_ROGOFF,
     "notes": "Louis XVI - precipitated French Revolution"},
    
    # Latin American Independence Defaults (1820s-1830s)
    {"country": "ARG", "type": CrisisType.SOVEREIGN, "start": 1827, "source": DataSource.REINHART_ROGOFF},
    {"country": "BRA", "type": CrisisType.SOVEREIGN, "start": 1828, "source": DataSource.REINHART_ROGOFF},
    {"country": "MEX", "type": CrisisType.SOVEREIGN, "start": 1827, "source": DataSource.REINHART_ROGOFF},
    {"country": "COL", "type": CrisisType.SOVEREIGN, "start": 1826, "source": DataSource.REINHART_ROGOFF},
    {"country": "PER", "type": CrisisType.SOVEREIGN, "start": 1826, "source": DataSource.REINHART_ROGOFF},
    {"country": "CHL", "type": CrisisType.SOVEREIGN, "start": 1826, "source": DataSource.REINHART_ROGOFF},
    {"country": "ECU", "type": CrisisType.SOVEREIGN, "start": 1826, "source": DataSource.REINHART_ROGOFF},
    {"country": "VEN", "type": CrisisType.SOVEREIGN, "start": 1826, "source": DataSource.REINHART_ROGOFF},
    
    # Major Banking Crises
    {"country": "GBR", "type": CrisisType.BANKING, "start": 1825, "source": DataSource.REINHART_ROGOFF,
     "notes": "Latin American speculation bubble burst"},
    {"country": "USA", "type": CrisisType.BANKING, "start": 1857, "source": DataSource.REINHART_ROGOFF,
     "notes": "Panic of 1857 - railroad speculation"},
    {"country": "GBR", "type": CrisisType.BANKING, "start": 1866, "source": DataSource.REINHART_ROGOFF,
     "notes": "Overend, Gurney & Co collapse"},
    {"country": "USA", "type": CrisisType.BANKING, "start": 1873, "source": DataSource.REINHART_ROGOFF,
     "notes": "Long Depression begins"},
    {"country": "ARG", "type": CrisisType.BANKING, "start": 1890, "source": DataSource.REINHART_ROGOFF,
     "notes": "Baring Crisis"},
    {"country": "USA", "type": CrisisType.BANKING, "start": 1907, "source": DataSource.REINHART_ROGOFF,
     "notes": "Panic of 1907 - led to Federal Reserve creation"},
    {"country": "AUT", "type": CrisisType.BANKING, "start": 1931, "source": DataSource.REINHART_ROGOFF,
     "notes": "Credit-Anstalt collapse - triggered European crisis"},
    {"country": "DEU", "type": CrisisType.BANKING, "start": 1931, "source": DataSource.REINHART_ROGOFF,
     "notes": "Danat Bank failure"},
    {"country": "USA", "type": CrisisType.BANKING, "start": 1933, "source": DataSource.REINHART_ROGOFF,
     "notes": "Great Depression banking crisis"},
    
    # Hyperinflations
    {"country": "DEU", "type": CrisisType.INFLATION, "start": 1920, "end": 1923, "source": DataSource.REINHART_ROGOFF,
     "notes": "Weimar hyperinflation", "peak_inflation": 29500},
    {"country": "HUN", "type": CrisisType.INFLATION, "start": 1945, "end": 1946, "source": DataSource.REINHART_ROGOFF,
     "notes": "Worst hyperinflation ever recorded", "peak_inflation": 4.19e16},
    {"country": "ARG", "type": CrisisType.INFLATION, "start": 1989, "end": 1990, "source": DataSource.REINHART_ROGOFF,
     "notes": "Argentine hyperinflation", "peak_inflation": 3079},
    {"country": "BRA", "type": CrisisType.INFLATION, "start": 1989, "end": 1994, "source": DataSource.REINHART_ROGOFF,
     "notes": "Brazilian hyperinflation", "peak_inflation": 2947},
    {"country": "ZWE", "type": CrisisType.INFLATION, "start": 2007, "end": 2008, "source": DataSource.REINHART_ROGOFF,
     "notes": "Zimbabwe hyperinflation", "peak_inflation": 7.96e10},
    {"country": "VEN", "type": CrisisType.INFLATION, "start": 2016, "source": DataSource.REINHART_ROGOFF,
     "notes": "Venezuelan hyperinflation - ongoing", "peak_inflation": 130000},
]

# Laeven-Valencia crises (modern, with cost data)
LAEVEN_VALENCIA_CRISES = [
    # 1990s Crises
    {"country": "MEX", "type": CrisisType.BANKING, "start": 1994, "end": 1997, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 13.3, "fiscal_cost": 19.3, "notes": "Tequila Crisis"},
    {"country": "THA", "type": CrisisType.BANKING, "start": 1997, "end": 2000, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 109.3, "fiscal_cost": 43.8, "notes": "Asian Financial Crisis"},
    {"country": "KOR", "type": CrisisType.BANKING, "start": 1997, "end": 1998, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 50.1, "fiscal_cost": 31.2, "notes": "Asian Financial Crisis"},
    {"country": "IDN", "type": CrisisType.BANKING, "start": 1997, "end": 2001, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 69.0, "fiscal_cost": 56.8, "notes": "Asian Financial Crisis"},
    {"country": "MYS", "type": CrisisType.BANKING, "start": 1997, "end": 1999, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 31.4, "fiscal_cost": 16.4, "notes": "Asian Financial Crisis"},
    {"country": "RUS", "type": CrisisType.BANKING, "start": 1998, "end": 1998, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 0, "fiscal_cost": 5.7, "notes": "Russian default + currency crisis"},
    {"country": "ARG", "type": CrisisType.BANKING, "start": 2001, "end": 2003, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 42.7, "fiscal_cost": 9.6, "notes": "Argentine default + currency crisis"},
    
    # Global Financial Crisis (2007-2009)
    {"country": "USA", "type": CrisisType.BANKING, "start": 2007, "end": 2009, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 31.0, "fiscal_cost": 4.5, "notes": "Global Financial Crisis"},
    {"country": "GBR", "type": CrisisType.BANKING, "start": 2007, "end": 2009, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 23.8, "fiscal_cost": 8.8, "notes": "Global Financial Crisis"},
    {"country": "DEU", "type": CrisisType.BANKING, "start": 2008, "end": 2009, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 5.6, "fiscal_cost": 1.8, "notes": "Global Financial Crisis"},
    {"country": "FRA", "type": CrisisType.BANKING, "start": 2008, "end": 2009, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 3.0, "fiscal_cost": 1.0, "notes": "Global Financial Crisis"},
    {"country": "IRL", "type": CrisisType.BANKING, "start": 2008, "end": 2012, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 106.0, "fiscal_cost": 40.7, "notes": "Irish banking crisis"},
    {"country": "ISL", "type": CrisisType.BANKING, "start": 2008, "end": 2010, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 43.0, "fiscal_cost": 44.0, "notes": "Icelandic banking collapse"},
    
    # European Sovereign Debt Crisis
    {"country": "GRC", "type": CrisisType.BANKING, "start": 2008, "end": 2012, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 43.0, "fiscal_cost": 27.3, "notes": "Greek debt crisis"},
    {"country": "ESP", "type": CrisisType.BANKING, "start": 2008, "end": 2012, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 37.3, "fiscal_cost": 5.4, "notes": "Spanish banking crisis"},
    {"country": "CYP", "type": CrisisType.BANKING, "start": 2011, "end": 2015, "source": DataSource.LAEVEN_VALENCIA,
     "output_loss": 23.8, "fiscal_cost": 10.6, "notes": "Cyprus banking crisis"},
    
    # Currency Crises
    {"country": "GBR", "type": CrisisType.CURRENCY, "start": 1992, "source": DataSource.LAEVEN_VALENCIA,
     "notes": "Black Wednesday - ERM exit", "depreciation": 15},
    {"country": "THA", "type": CrisisType.CURRENCY, "start": 1997, "source": DataSource.LAEVEN_VALENCIA,
     "notes": "Thai baht collapse", "depreciation": 50},
    {"country": "RUS", "type": CrisisType.CURRENCY, "start": 1998, "source": DataSource.LAEVEN_VALENCIA,
     "notes": "Ruble devaluation", "depreciation": 75},
    {"country": "ARG", "type": CrisisType.CURRENCY, "start": 2002, "source": DataSource.LAEVEN_VALENCIA,
     "notes": "Peso collapse - end of convertibility", "depreciation": 70},
    {"country": "TUR", "type": CrisisType.CURRENCY, "start": 2018, "source": DataSource.LAEVEN_VALENCIA,
     "notes": "Turkish lira crisis", "depreciation": 45},
    {"country": "ARG", "type": CrisisType.CURRENCY, "start": 2018, "source": DataSource.LAEVEN_VALENCIA,
     "notes": "Peso crisis", "depreciation": 50},
]


# =============================================================================
# LOADER CLASS
# =============================================================================

class CrisisLoader:
    """Load financial crisis data into PostgreSQL."""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def load_sample_data(self) -> dict:
        """Load all sample crisis data."""
        results = {
            "reinhart_rogoff": self._load_crises(REINHART_ROGOFF_CRISES),
            "laeven_valencia": self._load_crises(LAEVEN_VALENCIA_CRISES),
        }
        return results
    
    def _load_crises(self, crises: list) -> int:
        """Load list of crises into database."""
        count = 0
        
        with self.db.session() as session:
            for c in crises:
                # Check if exists
                existing = session.query(FinancialCrisis).filter_by(
                    country_iso3=c["country"],
                    crisis_type=c["type"],
                    start_year=c["start"],
                ).first()
                
                if existing:
                    continue
                
                crisis = FinancialCrisis(
                    country_iso3=c["country"],
                    crisis_type=c["type"],
                    start_year=c["start"],
                    end_year=c.get("end"),
                    source=c["source"],
                    source_notes=c.get("notes"),
                    output_loss_pct=c.get("output_loss"),
                    fiscal_cost_pct=c.get("fiscal_cost"),
                    peak_inflation=c.get("peak_inflation"),
                    exchange_rate_depreciation=c.get("depreciation"),
                )
                session.add(crisis)
                count += 1
        
        return count
    
    def get_stats(self) -> dict:
        """Get loading statistics."""
        with self.db.session() as session:
            from sqlalchemy import text
            
            total = session.query(FinancialCrisis).count()
            
            by_type = session.execute(text("""
                SELECT crisis_type, COUNT(*) as cnt
                FROM financial_crises
                GROUP BY crisis_type
                ORDER BY cnt DESC
            """))
            
            by_source = session.execute(text("""
                SELECT source, COUNT(*) as cnt
                FROM financial_crises
                GROUP BY source
            """))
            
            by_decade = session.execute(text("""
                SELECT (start_year / 10) * 10 as decade, COUNT(*) as cnt
                FROM financial_crises
                GROUP BY decade
                ORDER BY decade DESC
                LIMIT 10
            """))
        
        return {
            "total_crises": total,
            "by_type": [(row[0], row[1]) for row in by_type],
            "by_source": [(row[0], row[1]) for row in by_source],
            "by_decade": [(row[0], row[1]) for row in by_decade],
        }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Financial Crisis Loader")
    parser.add_argument("--sample", action="store_true", help="Load sample data")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    
    args = parser.parse_args()
    
    loader = CrisisLoader()
    
    print("=" * 60)
    print("Financial Crisis Data Loader")
    print("=" * 60)
    print(f"\nDatabase Connected: {loader.db.test_connection()}")
    
    if args.sample:
        print("\nLoading sample data...")
        results = loader.load_sample_data()
        print(f"✅ Reinhart-Rogoff: {results['reinhart_rogoff']} crises")
        print(f"✅ Laeven-Valencia: {results['laeven_valencia']} crises")
    
    if args.stats:
        print("\n📊 Database Statistics:")
        stats = loader.get_stats()
        print(f"  Total crises: {stats['total_crises']}")
        
        if stats['by_type']:
            print("\n  By type:")
            for ctype, cnt in stats['by_type']:
                print(f"    {ctype}: {cnt}")
        
        if stats['by_source']:
            print("\n  By source:")
            for src, cnt in stats['by_source']:
                print(f"    {src}: {cnt}")
    
    if not any([args.sample, args.stats]):
        parser.print_help()


if __name__ == "__main__":
    main()
