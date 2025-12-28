"""
Database Initialization Script.

Creates tables and loads initial reference data.

Usage:
    python -m database.init_db --create-tables
    python -m database.init_db --load-countries
    python -m database.init_db --load-all
    python -m database.init_db --drop-tables  # DANGER!
"""

import argparse
import sys
from datetime import datetime

from sqlalchemy import text

from .connection import DatabaseManager
from .models import (
    Base, create_all_tables, drop_all_tables,
    Country, Indicator, Region, IncomeGroup, DataSource
)


# =============================================================================
# REFERENCE DATA
# =============================================================================

COUNTRIES_DATA = [
    # North America
    ("USA", "US", "United States", "United States of America", Region.NORTH_AMERICA, IncomeGroup.HIGH, "Washington D.C.", "USD", 38.9072, -77.0369),
    ("CAN", "CA", "Canada", None, Region.NORTH_AMERICA, IncomeGroup.HIGH, "Ottawa", "CAD", 45.4215, -75.6972),
    ("MEX", "MX", "Mexico", "United Mexican States", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Mexico City", "MXN", 19.4326, -99.1332),
    
    # Latin America
    ("BRA", "BR", "Brazil", "Federative Republic of Brazil", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Brasília", "BRL", -15.7975, -47.8919),
    ("ARG", "AR", "Argentina", "Argentine Republic", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Buenos Aires", "ARS", -34.6037, -58.3816),
    ("CHL", "CL", "Chile", "Republic of Chile", Region.LATIN_AMERICA, IncomeGroup.HIGH, "Santiago", "CLP", -33.4489, -70.6693),
    ("COL", "CO", "Colombia", "Republic of Colombia", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Bogotá", "COP", 4.7110, -74.0721),
    ("PER", "PE", "Peru", "Republic of Peru", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Lima", "PEN", -12.0464, -77.0428),
    ("VEN", "VE", "Venezuela", "Bolivarian Republic of Venezuela", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Caracas", "VES", 10.4806, -66.9036),
    ("ECU", "EC", "Ecuador", "Republic of Ecuador", Region.LATIN_AMERICA, IncomeGroup.UPPER_MIDDLE, "Quito", "USD", -0.1807, -78.4678),
    ("HTI", "HT", "Haiti", "Republic of Haiti", Region.LATIN_AMERICA, IncomeGroup.LOW, "Port-au-Prince", "HTG", 18.5944, -72.3074),
    
    # Europe
    ("GBR", "GB", "United Kingdom", "United Kingdom of Great Britain and Northern Ireland", Region.EUROPE, IncomeGroup.HIGH, "London", "GBP", 51.5074, -0.1278),
    ("DEU", "DE", "Germany", "Federal Republic of Germany", Region.EUROPE, IncomeGroup.HIGH, "Berlin", "EUR", 52.5200, 13.4050),
    ("FRA", "FR", "France", "French Republic", Region.EUROPE, IncomeGroup.HIGH, "Paris", "EUR", 48.8566, 2.3522),
    ("ITA", "IT", "Italy", "Italian Republic", Region.EUROPE, IncomeGroup.HIGH, "Rome", "EUR", 41.9028, 12.4964),
    ("ESP", "ES", "Spain", "Kingdom of Spain", Region.EUROPE, IncomeGroup.HIGH, "Madrid", "EUR", 40.4168, -3.7038),
    ("NLD", "NL", "Netherlands", "Kingdom of the Netherlands", Region.EUROPE, IncomeGroup.HIGH, "Amsterdam", "EUR", 52.3676, 4.9041),
    ("CHE", "CH", "Switzerland", "Swiss Confederation", Region.EUROPE, IncomeGroup.HIGH, "Bern", "CHF", 46.9480, 7.4474),
    ("SWE", "SE", "Sweden", "Kingdom of Sweden", Region.EUROPE, IncomeGroup.HIGH, "Stockholm", "SEK", 59.3293, 18.0686),
    ("POL", "PL", "Poland", "Republic of Poland", Region.EUROPE, IncomeGroup.HIGH, "Warsaw", "PLN", 52.2297, 21.0122),
    ("GRC", "GR", "Greece", "Hellenic Republic", Region.EUROPE, IncomeGroup.HIGH, "Athens", "EUR", 37.9838, 23.7275),
    ("TUR", "TR", "Turkey", "Republic of Turkey", Region.EUROPE, IncomeGroup.UPPER_MIDDLE, "Ankara", "TRY", 39.9334, 32.8597),
    ("RUS", "RU", "Russia", "Russian Federation", Region.EUROPE, IncomeGroup.UPPER_MIDDLE, "Moscow", "RUB", 55.7558, 37.6173),
    ("UKR", "UA", "Ukraine", None, Region.EUROPE, IncomeGroup.LOWER_MIDDLE, "Kyiv", "UAH", 50.4501, 30.5234),
    
    # Asia
    ("CHN", "CN", "China", "People's Republic of China", Region.EAST_ASIA, IncomeGroup.UPPER_MIDDLE, "Beijing", "CNY", 39.9042, 116.4074),
    ("JPN", "JP", "Japan", None, Region.EAST_ASIA, IncomeGroup.HIGH, "Tokyo", "JPY", 35.6762, 139.6503),
    ("KOR", "KR", "South Korea", "Republic of Korea", Region.EAST_ASIA, IncomeGroup.HIGH, "Seoul", "KRW", 37.5665, 126.9780),
    ("IND", "IN", "India", "Republic of India", Region.SOUTH_ASIA, IncomeGroup.LOWER_MIDDLE, "New Delhi", "INR", 28.6139, 77.2090),
    ("IDN", "ID", "Indonesia", "Republic of Indonesia", Region.EAST_ASIA, IncomeGroup.UPPER_MIDDLE, "Jakarta", "IDR", -6.2088, 106.8456),
    ("THA", "TH", "Thailand", "Kingdom of Thailand", Region.EAST_ASIA, IncomeGroup.UPPER_MIDDLE, "Bangkok", "THB", 13.7563, 100.5018),
    ("VNM", "VN", "Vietnam", "Socialist Republic of Vietnam", Region.EAST_ASIA, IncomeGroup.LOWER_MIDDLE, "Hanoi", "VND", 21.0278, 105.8342),
    ("PHL", "PH", "Philippines", "Republic of the Philippines", Region.EAST_ASIA, IncomeGroup.LOWER_MIDDLE, "Manila", "PHP", 14.5995, 120.9842),
    ("MYS", "MY", "Malaysia", None, Region.EAST_ASIA, IncomeGroup.UPPER_MIDDLE, "Kuala Lumpur", "MYR", 3.1390, 101.6869),
    ("SGP", "SG", "Singapore", "Republic of Singapore", Region.EAST_ASIA, IncomeGroup.HIGH, "Singapore", "SGD", 1.3521, 103.8198),
    ("PAK", "PK", "Pakistan", "Islamic Republic of Pakistan", Region.SOUTH_ASIA, IncomeGroup.LOWER_MIDDLE, "Islamabad", "PKR", 33.6844, 73.0479),
    ("BGD", "BD", "Bangladesh", "People's Republic of Bangladesh", Region.SOUTH_ASIA, IncomeGroup.LOWER_MIDDLE, "Dhaka", "BDT", 23.8103, 90.4125),
    
    # Middle East
    ("SAU", "SA", "Saudi Arabia", "Kingdom of Saudi Arabia", Region.MIDDLE_EAST, IncomeGroup.HIGH, "Riyadh", "SAR", 24.7136, 46.6753),
    ("ARE", "AE", "United Arab Emirates", None, Region.MIDDLE_EAST, IncomeGroup.HIGH, "Abu Dhabi", "AED", 24.4539, 54.3773),
    ("IRN", "IR", "Iran", "Islamic Republic of Iran", Region.MIDDLE_EAST, IncomeGroup.LOWER_MIDDLE, "Tehran", "IRR", 35.6892, 51.3890),
    ("ISR", "IL", "Israel", "State of Israel", Region.MIDDLE_EAST, IncomeGroup.HIGH, "Jerusalem", "ILS", 31.7683, 35.2137),
    ("EGY", "EG", "Egypt", "Arab Republic of Egypt", Region.MIDDLE_EAST, IncomeGroup.LOWER_MIDDLE, "Cairo", "EGP", 30.0444, 31.2357),
    
    # Africa
    ("ZAF", "ZA", "South Africa", "Republic of South Africa", Region.SUB_SAHARAN_AFRICA, IncomeGroup.UPPER_MIDDLE, "Pretoria", "ZAR", -25.7479, 28.2293),
    ("NGA", "NG", "Nigeria", "Federal Republic of Nigeria", Region.SUB_SAHARAN_AFRICA, IncomeGroup.LOWER_MIDDLE, "Abuja", "NGN", 9.0765, 7.3986),
    ("KEN", "KE", "Kenya", "Republic of Kenya", Region.SUB_SAHARAN_AFRICA, IncomeGroup.LOWER_MIDDLE, "Nairobi", "KES", -1.2921, 36.8219),
    ("ETH", "ET", "Ethiopia", "Federal Democratic Republic of Ethiopia", Region.SUB_SAHARAN_AFRICA, IncomeGroup.LOW, "Addis Ababa", "ETB", 9.0320, 38.7469),
    
    # Oceania
    ("AUS", "AU", "Australia", "Commonwealth of Australia", Region.OCEANIA, IncomeGroup.HIGH, "Canberra", "AUD", -35.2809, 149.1300),
    ("NZL", "NZ", "New Zealand", None, Region.OCEANIA, IncomeGroup.HIGH, "Wellington", "NZD", -41.2865, 174.7762),
]


INDICATORS_DATA = [
    # Economic - World Bank
    ("GDP_CURRENT", "GDP (current US$)", DataSource.WORLD_BANK, "NY.GDP.MKTP.CD", "Economic", "GDP", "Current US$", "Annual"),
    ("GDP_GROWTH", "GDP growth (annual %)", DataSource.WORLD_BANK, "NY.GDP.MKTP.KD.ZG", "Economic", "GDP", "Percent", "Annual"),
    ("GDP_PER_CAPITA", "GDP per capita (current US$)", DataSource.WORLD_BANK, "NY.GDP.PCAP.CD", "Economic", "GDP", "Current US$", "Annual"),
    ("INFLATION_CPI", "Inflation, consumer prices (annual %)", DataSource.WORLD_BANK, "FP.CPI.TOTL.ZG", "Economic", "Prices", "Percent", "Annual"),
    ("UNEMPLOYMENT", "Unemployment, total (% of labor force)", DataSource.WORLD_BANK, "SL.UEM.TOTL.ZS", "Economic", "Labor", "Percent", "Annual"),
    ("TRADE_PCT_GDP", "Trade (% of GDP)", DataSource.WORLD_BANK, "NE.TRD.GNFS.ZS", "Economic", "Trade", "Percent", "Annual"),
    ("FDI_INFLOWS", "Foreign direct investment, net inflows (% of GDP)", DataSource.WORLD_BANK, "BX.KLT.DINV.WD.GD.ZS", "Economic", "Investment", "Percent", "Annual"),
    ("DEBT_GOVT_PCT_GDP", "Central government debt, total (% of GDP)", DataSource.WORLD_BANK, "GC.DOD.TOTL.GD.ZS", "Economic", "Debt", "Percent", "Annual"),
    
    # Demographics
    ("POPULATION", "Population, total", DataSource.WORLD_BANK, "SP.POP.TOTL", "Demographics", "Population", "Number", "Annual"),
    ("POP_GROWTH", "Population growth (annual %)", DataSource.WORLD_BANK, "SP.POP.GROW", "Demographics", "Population", "Percent", "Annual"),
    ("URBAN_POP_PCT", "Urban population (% of total)", DataSource.WORLD_BANK, "SP.URB.TOTL.IN.ZS", "Demographics", "Urbanization", "Percent", "Annual"),
    ("LIFE_EXPECTANCY", "Life expectancy at birth, total (years)", DataSource.WORLD_BANK, "SP.DYN.LE00.IN", "Demographics", "Health", "Years", "Annual"),
    
    # FRED - Interest Rates
    ("FEDFUNDS", "Federal Funds Effective Rate", DataSource.FRED, "FEDFUNDS", "Monetary Policy", "Interest Rates", "Percent", "Monthly"),
    ("DGS10", "10-Year Treasury Constant Maturity Rate", DataSource.FRED, "DGS10", "Monetary Policy", "Interest Rates", "Percent", "Daily"),
    ("DGS2", "2-Year Treasury Constant Maturity Rate", DataSource.FRED, "DGS2", "Monetary Policy", "Interest Rates", "Percent", "Daily"),
    ("T10Y2Y", "10-Year Treasury Minus 2-Year Treasury", DataSource.FRED, "T10Y2Y", "Monetary Policy", "Interest Rates", "Percent", "Daily"),
    
    # FRED - Inflation
    ("CPIAUCSL", "Consumer Price Index for All Urban Consumers", DataSource.FRED, "CPIAUCSL", "Inflation", "Prices", "Index", "Monthly"),
    ("PCEPI", "Personal Consumption Expenditures Price Index", DataSource.FRED, "PCEPI", "Inflation", "Prices", "Index", "Monthly"),
    
    # FRED - Fed Balance Sheet
    ("WALCL", "Federal Reserve Total Assets", DataSource.FRED, "WALCL", "Monetary Policy", "Fed Balance Sheet", "Millions USD", "Weekly"),
    ("M2SL", "M2 Money Stock", DataSource.FRED, "M2SL", "Monetary Policy", "Money Supply", "Billions USD", "Monthly"),
]


# =============================================================================
# INITIALIZATION FUNCTIONS
# =============================================================================

def create_tables(db: DatabaseManager) -> bool:
    """Create all database tables."""
    try:
        create_all_tables(db.engine)
        print("✅ All tables created successfully")
        return True
    except Exception as e:
        print(f"❌ Error creating tables: {e}")
        return False


def drop_tables(db: DatabaseManager) -> bool:
    """Drop all tables (DANGER!)."""
    confirm = input("⚠️  This will DELETE ALL DATA. Type 'DELETE' to confirm: ")
    if confirm != "DELETE":
        print("Aborted.")
        return False
    
    try:
        drop_all_tables(db.engine)
        print("✅ All tables dropped")
        return True
    except Exception as e:
        print(f"❌ Error dropping tables: {e}")
        return False


def load_countries(db: DatabaseManager) -> int:
    """Load country reference data."""
    count = 0
    
    with db.session() as session:
        for row in COUNTRIES_DATA:
            iso3, iso2, name, official, region, income, capital, currency, lat, lon = row
            
            # Check if exists
            existing = session.query(Country).filter_by(iso3=iso3).first()
            if existing:
                continue
            
            country = Country(
                iso3=iso3,
                iso2=iso2,
                name=name,
                official_name=official,
                region=region,
                income_group=income,
                capital=capital,
                currency_code=currency,
                latitude=lat,
                longitude=lon,
            )
            session.add(country)
            count += 1
    
    print(f"✅ Loaded {count} countries")
    return count


def load_indicators(db: DatabaseManager) -> int:
    """Load indicator metadata."""
    count = 0
    
    with db.session() as session:
        for row in INDICATORS_DATA:
            code, name, source, source_code, category, subcategory, unit, frequency = row
            
            # Check if exists
            existing = session.query(Indicator).filter_by(code=code).first()
            if existing:
                continue
            
            indicator = Indicator(
                code=code,
                name=name,
                source=source,
                source_code=source_code,
                category=category,
                subcategory=subcategory,
                unit=unit,
                frequency=frequency,
            )
            session.add(indicator)
            count += 1
    
    print(f"✅ Loaded {count} indicators")
    return count


def verify_tables(db: DatabaseManager) -> dict:
    """Verify table existence and row counts."""
    results = {}
    
    with db.session() as session:
        tables = ["countries", "indicators", "economic_data", "fed_series", "disasters", "financial_crises"]
        
        for table in tables:
            try:
                result = session.execute(text(f"SELECT COUNT(*) FROM {table}"))
                count = result.scalar()
                results[table] = count
            except Exception as e:
                results[table] = f"ERROR: {e}"
    
    return results


# =============================================================================
# CLI
# =============================================================================

def main():
    parser = argparse.ArgumentParser(description="Database Initialization")
    parser.add_argument("--create-tables", action="store_true", help="Create all tables")
    parser.add_argument("--drop-tables", action="store_true", help="Drop all tables (DANGER!)")
    parser.add_argument("--load-countries", action="store_true", help="Load country data")
    parser.add_argument("--load-indicators", action="store_true", help="Load indicator metadata")
    parser.add_argument("--load-all", action="store_true", help="Load all reference data")
    parser.add_argument("--verify", action="store_true", help="Verify tables and counts")
    parser.add_argument("--init", action="store_true", help="Full initialization (create + load)")
    
    args = parser.parse_args()
    
    db = DatabaseManager()
    
    print("=" * 60)
    print("Database Initialization")
    print("=" * 60)
    
    info = db.get_info()
    print(f"\nDatabase: {info['url']}")
    print(f"Connected: {info['connected']}")
    
    if not info['connected']:
        print("\n❌ Cannot connect to database. Check configuration.")
        sys.exit(1)
    
    if args.drop_tables:
        drop_tables(db)
    
    if args.create_tables or args.init:
        create_tables(db)
    
    if args.load_countries or args.load_all or args.init:
        load_countries(db)
    
    if args.load_indicators or args.load_all or args.init:
        load_indicators(db)
    
    if args.verify or args.init:
        print("\n📊 Table Statistics:")
        results = verify_tables(db)
        for table, count in results.items():
            print(f"  - {table}: {count} rows")
    
    if not any(vars(args).values()):
        parser.print_help()


if __name__ == "__main__":
    main()
