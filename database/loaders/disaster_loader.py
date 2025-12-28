"""
EM-DAT Disaster Data Loader.

Loads disaster data into PostgreSQL.

Supports:
- Sample data loading (curated historical events)
- Excel file import (from EM-DAT download)

Usage:
    python -m database.loaders.disaster_loader --sample
    python -m database.loaders.disaster_loader --file emdat_public.xlsx
    python -m database.loaders.disaster_loader --stats
"""

import argparse
import os
from datetime import date
from decimal import Decimal
from typing import Optional

import pandas as pd

import sys
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(__file__))))

from database.connection import DatabaseManager
from database.models import Disaster, DisasterType, DisasterGroup


# =============================================================================
# SAMPLE DATA
# =============================================================================

SAMPLE_DISASTERS = [
    # Earthquakes
    {"year": 1906, "country": "USA", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "San Francisco Earthquake", "deaths": 3000, "affected": 225000, "damage": 500, "magnitude": 7.9},
    {"year": 1923, "country": "JPN", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Great Kanto Earthquake", "deaths": 142800, "affected": 3400000, "damage": 2800, "magnitude": 7.9},
    {"year": 1960, "country": "CHL", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Great Chilean Earthquake", "deaths": 5700, "affected": 2000000, "damage": 675, "magnitude": 9.5},
    {"year": 1976, "country": "CHN", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Tangshan Earthquake", "deaths": 242419, "affected": 164000, "damage": 5600, "magnitude": 7.5},
    {"year": 1985, "country": "MEX", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Mexico City Earthquake", "deaths": 9500, "affected": 2130204, "damage": 4104, "magnitude": 8.0},
    {"year": 1995, "country": "JPN", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Kobe Earthquake", "deaths": 5297, "affected": 541636, "damage": 100000, "magnitude": 6.9},
    {"year": 2004, "country": "IDN", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Indian Ocean Earthquake/Tsunami", "deaths": 226000, "affected": 2500000, "damage": 15000, "magnitude": 9.1},
    {"year": 2008, "country": "CHN", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Sichuan Earthquake", "deaths": 87476, "affected": 45976596, "damage": 85000, "magnitude": 7.9},
    {"year": 2010, "country": "HTI", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Haiti Earthquake", "deaths": 222570, "affected": 3700000, "damage": 8000, "magnitude": 7.0},
    {"year": 2011, "country": "JPN", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Tohoku Earthquake/Tsunami", "deaths": 19846, "affected": 368820, "damage": 210000, "magnitude": 9.1},
    {"year": 2015, "country": "NPL", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Nepal Earthquake", "deaths": 8831, "affected": 5639724, "damage": 5150, "magnitude": 7.8},
    {"year": 2023, "country": "TUR", "type": DisasterType.EARTHQUAKE, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Turkey-Syria Earthquake", "deaths": 53227, "affected": 15732231, "damage": 34200, "magnitude": 7.8},
    
    # Tropical Cyclones
    {"year": 1900, "country": "USA", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Galveston Hurricane", "deaths": 8000, "affected": 30000, "damage": 30},
    {"year": 1970, "country": "BGD", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Bhola Cyclone", "deaths": 300000, "affected": 3648000, "damage": 86},
    {"year": 1991, "country": "BGD", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Bangladesh Cyclone", "deaths": 138866, "affected": 15438849, "damage": 1780},
    {"year": 1998, "country": "HND", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Hurricane Mitch", "deaths": 14600, "affected": 2112000, "damage": 5000},
    {"year": 2005, "country": "USA", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Hurricane Katrina", "deaths": 1833, "affected": 500000, "damage": 125000},
    {"year": 2008, "country": "MMR", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Cyclone Nargis", "deaths": 138366, "affected": 2420000, "damage": 10000},
    {"year": 2012, "country": "USA", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Hurricane Sandy", "deaths": 159, "affected": 8900000, "damage": 68000},
    {"year": 2013, "country": "PHL", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Typhoon Haiyan", "deaths": 7354, "affected": 16078181, "damage": 12900},
    {"year": 2017, "country": "USA", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Hurricane Harvey", "deaths": 89, "affected": 13000000, "damage": 125000},
    {"year": 2017, "country": "PRI", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Hurricane Maria", "deaths": 2975, "affected": 3337177, "damage": 91610},
    {"year": 2024, "country": "USA", "type": DisasterType.TROPICAL_CYCLONE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Hurricane Helene", "deaths": 232, "affected": 1500000, "damage": 53000},
    
    # Floods
    {"year": 1931, "country": "CHN", "type": DisasterType.FLOOD, "group": DisasterGroup.HYDROLOGICAL,
     "name": "China Floods", "deaths": 3700000, "affected": 28500000, "damage": None},
    {"year": 1998, "country": "CHN", "type": DisasterType.FLOOD, "group": DisasterGroup.HYDROLOGICAL,
     "name": "Yangtze River Floods", "deaths": 3656, "affected": 238973000, "damage": 30000},
    {"year": 2010, "country": "PAK", "type": DisasterType.FLOOD, "group": DisasterGroup.HYDROLOGICAL,
     "name": "Pakistan Floods", "deaths": 1985, "affected": 20000000, "damage": 9500},
    {"year": 2011, "country": "THA", "type": DisasterType.FLOOD, "group": DisasterGroup.HYDROLOGICAL,
     "name": "Thailand Floods", "deaths": 815, "affected": 13592557, "damage": 45000},
    {"year": 2021, "country": "DEU", "type": DisasterType.FLOOD, "group": DisasterGroup.HYDROLOGICAL,
     "name": "European Floods", "deaths": 196, "affected": 183000, "damage": 40000},
    {"year": 2022, "country": "PAK", "type": DisasterType.FLOOD, "group": DisasterGroup.HYDROLOGICAL,
     "name": "Pakistan Monsoon Floods", "deaths": 1739, "affected": 33000000, "damage": 15000},
    
    # Droughts
    {"year": 1983, "country": "ETH", "type": DisasterType.DROUGHT, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "Ethiopian Famine", "deaths": 300000, "affected": 7750000, "damage": None},
    {"year": 2011, "country": "SOM", "type": DisasterType.DROUGHT, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "East Africa Drought", "deaths": 258000, "affected": 13000000, "damage": None},
    {"year": 2012, "country": "USA", "type": DisasterType.DROUGHT, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "US Drought", "deaths": 123, "affected": None, "damage": 30000},
    
    # Heat Waves
    {"year": 2003, "country": "FRA", "type": DisasterType.HEAT_WAVE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "European Heat Wave", "deaths": 70000, "affected": None, "damage": 13100},
    {"year": 2010, "country": "RUS", "type": DisasterType.HEAT_WAVE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "Russian Heat Wave", "deaths": 55736, "affected": None, "damage": 15000},
    {"year": 2022, "country": "FRA", "type": DisasterType.HEAT_WAVE, "group": DisasterGroup.METEOROLOGICAL,
     "name": "European Heat Wave", "deaths": 61672, "affected": None, "damage": 20000},
    
    # Wildfires
    {"year": 2009, "country": "AUS", "type": DisasterType.WILDFIRE, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "Black Saturday Fires", "deaths": 180, "affected": 7562, "damage": 4400},
    {"year": 2018, "country": "USA", "type": DisasterType.WILDFIRE, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "Camp Fire", "deaths": 85, "affected": 52000, "damage": 16500},
    {"year": 2019, "country": "AUS", "type": DisasterType.WILDFIRE, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "Australian Bushfires", "deaths": 34, "affected": 65000, "damage": 100000},
    {"year": 2023, "country": "CAN", "type": DisasterType.WILDFIRE, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "Canadian Wildfires", "deaths": 8, "affected": 232000, "damage": 3000},
    {"year": 2023, "country": "USA", "type": DisasterType.WILDFIRE, "group": DisasterGroup.CLIMATOLOGICAL,
     "name": "Maui Wildfire", "deaths": 100, "affected": 11000, "damage": 5500},
    
    # Volcanic
    {"year": 1985, "country": "COL", "type": DisasterType.VOLCANIC, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Nevado del Ruiz", "deaths": 21800, "affected": 12700, "damage": 1000},
    {"year": 1991, "country": "PHL", "type": DisasterType.VOLCANIC, "group": DisasterGroup.GEOPHYSICAL,
     "name": "Mount Pinatubo", "deaths": 847, "affected": 2100000, "damage": 374},
    
    # Epidemics
    {"year": 1918, "country": "USA", "type": DisasterType.EPIDEMIC, "group": DisasterGroup.BIOLOGICAL,
     "name": "Spanish Flu", "deaths": 50000000, "affected": 500000000, "damage": None},
    {"year": 2014, "country": "LBR", "type": DisasterType.EPIDEMIC, "group": DisasterGroup.BIOLOGICAL,
     "name": "Ebola Outbreak", "deaths": 11323, "affected": 28616, "damage": 2200},
    {"year": 2020, "country": "USA", "type": DisasterType.EPIDEMIC, "group": DisasterGroup.BIOLOGICAL,
     "name": "COVID-19 Pandemic", "deaths": 7000000, "affected": 770000000, "damage": 12700000},
]


# =============================================================================
# LOADER CLASS
# =============================================================================

class DisasterLoader:
    """Load disaster data into PostgreSQL."""
    
    def __init__(self):
        self.db = DatabaseManager()
    
    def load_sample_data(self) -> int:
        """Load curated sample disasters."""
        count = 0
        
        with self.db.session() as session:
            for d in SAMPLE_DISASTERS:
                # Check if exists
                existing = session.query(Disaster).filter_by(
                    country_iso3=d["country"],
                    year=d["year"],
                    event_name=d["name"],
                ).first()
                
                if existing:
                    continue
                
                disaster = Disaster(
                    country_iso3=d["country"],
                    disaster_type=d["type"],
                    disaster_group=d["group"],
                    event_name=d["name"],
                    year=d["year"],
                    start_date=date(d["year"], 1, 1),
                    deaths=d.get("deaths"),
                    affected=d.get("affected"),
                    damage_usd=Decimal(str(d["damage"] * 1e6)) if d.get("damage") else None,
                    magnitude=d.get("magnitude"),
                )
                session.add(disaster)
                count += 1
        
        return count
    
    def load_excel(self, filepath: str) -> int:
        """
        Load EM-DAT data from Excel file.
        
        Expected columns (EM-DAT format):
        - Dis No, Year, Country, ISO, Disaster Group, Disaster Subgroup,
        - Disaster Type, Disaster Subtype, Event Name, Start Year/Month/Day,
        - Total Deaths, No Injured, No Affected, No Homeless, Total Affected,
        - Total Damages ('000 US$), etc.
        """
        df = pd.read_excel(filepath)
        
        # Standardize column names
        df.columns = df.columns.str.lower().str.replace(" ", "_").str.replace("'", "")
        
        count = 0
        
        with self.db.session() as session:
            for _, row in df.iterrows():
                try:
                    # Map disaster type
                    dtype_str = str(row.get("disaster_type", "")).lower()
                    dtype = self._map_disaster_type(dtype_str)
                    
                    if dtype is None:
                        continue
                    
                    # Map disaster group
                    dgroup_str = str(row.get("disaster_group", "")).lower()
                    dgroup = self._map_disaster_group(dgroup_str)
                    
                    # Parse dates
                    year = int(row.get("start_year") or row.get("year"))
                    month = int(row.get("start_month", 1) or 1)
                    day = int(row.get("start_day", 1) or 1)
                    
                    try:
                        start = date(year, month, day)
                    except:
                        start = date(year, 1, 1)
                    
                    # Parse damage
                    damage = row.get("total_damages_000_us$") or row.get("total_damages")
                    if pd.notna(damage):
                        damage = Decimal(str(float(damage) * 1000))  # Convert from thousands
                    else:
                        damage = None
                    
                    disaster = Disaster(
                        emdat_id=str(row.get("dis_no", "")),
                        country_iso3=row.get("iso", row.get("iso3", ""))[:3],
                        disaster_type=dtype,
                        disaster_group=dgroup,
                        disaster_subtype=row.get("disaster_subtype"),
                        event_name=row.get("event_name"),
                        year=year,
                        start_date=start,
                        location=row.get("location"),
                        deaths=int(row.get("total_deaths", 0) or 0) if pd.notna(row.get("total_deaths")) else None,
                        injured=int(row.get("no_injured", 0) or 0) if pd.notna(row.get("no_injured")) else None,
                        affected=int(row.get("no_affected", 0) or 0) if pd.notna(row.get("no_affected")) else None,
                        homeless=int(row.get("no_homeless", 0) or 0) if pd.notna(row.get("no_homeless")) else None,
                        total_affected=int(row.get("total_affected", 0) or 0) if pd.notna(row.get("total_affected")) else None,
                        damage_usd=damage,
                    )
                    session.add(disaster)
                    count += 1
                    
                except Exception as e:
                    print(f"Warning: Error processing row: {e}")
                    continue
        
        return count
    
    def _map_disaster_type(self, dtype_str: str) -> Optional[DisasterType]:
        """Map string to DisasterType enum."""
        mapping = {
            "earthquake": DisasterType.EARTHQUAKE,
            "volcanic activity": DisasterType.VOLCANIC,
            "volcano": DisasterType.VOLCANIC,
            "tsunami": DisasterType.TSUNAMI,
            "storm": DisasterType.STORM,
            "tropical cyclone": DisasterType.TROPICAL_CYCLONE,
            "flood": DisasterType.FLOOD,
            "drought": DisasterType.DROUGHT,
            "wildfire": DisasterType.WILDFIRE,
            "extreme temperature": DisasterType.HEAT_WAVE,
            "heat wave": DisasterType.HEAT_WAVE,
            "cold wave": DisasterType.COLD_WAVE,
            "landslide": DisasterType.LANDSLIDE,
            "epidemic": DisasterType.EPIDEMIC,
        }
        return mapping.get(dtype_str.lower())
    
    def _map_disaster_group(self, group_str: str) -> Optional[DisasterGroup]:
        """Map string to DisasterGroup enum."""
        mapping = {
            "geophysical": DisasterGroup.GEOPHYSICAL,
            "meteorological": DisasterGroup.METEOROLOGICAL,
            "hydrological": DisasterGroup.HYDROLOGICAL,
            "climatological": DisasterGroup.CLIMATOLOGICAL,
            "biological": DisasterGroup.BIOLOGICAL,
        }
        return mapping.get(group_str.lower())
    
    def get_stats(self) -> dict:
        """Get loading statistics."""
        with self.db.session() as session:
            from sqlalchemy import func, text
            
            total = session.query(Disaster).count()
            
            by_type = session.execute(text("""
                SELECT disaster_type, COUNT(*) as cnt, SUM(deaths) as total_deaths
                FROM disasters
                GROUP BY disaster_type
                ORDER BY cnt DESC
            """))
            
            by_year = session.execute(text("""
                SELECT year, COUNT(*) as cnt
                FROM disasters
                GROUP BY year
                ORDER BY year DESC
                LIMIT 10
            """))
        
        return {
            "total_disasters": total,
            "by_type": [(row[0], row[1], row[2]) for row in by_type],
            "recent_years": [(row[0], row[1]) for row in by_year],
        }


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="EM-DAT Disaster Loader")
    parser.add_argument("--sample", action="store_true", help="Load sample data")
    parser.add_argument("--file", type=str, help="Load from EM-DAT Excel file")
    parser.add_argument("--stats", action="store_true", help="Show statistics")
    
    args = parser.parse_args()
    
    loader = DisasterLoader()
    
    print("=" * 60)
    print("EM-DAT Disaster Loader")
    print("=" * 60)
    print(f"\nDatabase Connected: {loader.db.test_connection()}")
    
    if args.sample:
        print("\nLoading sample data...")
        count = loader.load_sample_data()
        print(f"✅ Loaded {count} disasters")
    
    if args.file:
        print(f"\nLoading from file: {args.file}")
        count = loader.load_excel(args.file)
        print(f"✅ Loaded {count} disasters")
    
    if args.stats:
        print("\n📊 Database Statistics:")
        stats = loader.get_stats()
        print(f"  Total disasters: {stats['total_disasters']}")
        if stats['by_type']:
            print("\n  By type:")
            for dtype, cnt, deaths in stats['by_type']:
                print(f"    {dtype}: {cnt} events, {deaths or 0:,} deaths")
    
    if not any([args.sample, args.file, args.stats]):
        parser.print_help()


if __name__ == "__main__":
    main()
