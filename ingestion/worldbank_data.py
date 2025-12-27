"""World Bank data ingestion module."""
import requests
import pandas as pd
from datetime import datetime

class WorldBankData:
    """Fetch data from World Bank API."""
    
    BASE_URL = "https://api.worldbank.org/v2"
    
    # Popular indicators
    INDICATORS = {
        "NY.GDP.MKTP.CD": "GDP (current US$)",
        "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
        "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
        "SP.POP.TOTL": "Population, total",
        "SP.POP.GROW": "Population growth (annual %)",
        "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
        "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
        "SI.POV.DDAY": "Poverty headcount ratio at $2.15/day",
        "SE.ADT.LITR.ZS": "Literacy rate, adult total",
        "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
    }
    
    def __init__(self, db_manager=None):
        self.db = db_manager
    
    def fetch_indicator(self, indicator_code, countries="all", start_year=2000, end_year=2023):
        """Fetch a single indicator from World Bank API."""
        url = f"{self.BASE_URL}/country/{countries}/indicator/{indicator_code}"
        params = {
            "format": "json",
            "date": f"{start_year}:{end_year}",
            "per_page": 10000
        }
        
        try:
            response = requests.get(url, params=params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            if len(data) < 2 or not data[1]:
                return pd.DataFrame()
            
            records = []
            for item in data[1]:
                if item['value'] is not None:
                    records.append({
                        'country_iso3': item['countryiso3code'],
                        'country': item['country']['value'],
                        'indicator_code': indicator_code,
                        'indicator_name': item['indicator']['value'],
                        'year': int(item['date']),
                        'value': float(item['value'])
                    })
            
            return pd.DataFrame(records)
        except Exception as e:
            print(f"Error fetching {indicator_code}: {e}")
            return pd.DataFrame()
    
    def fetch_all_indicators(self, countries="all", start_year=2000, end_year=2023):
        """Fetch all predefined indicators."""
        all_data = []
        
        for code, name in self.INDICATORS.items():
            print(f"Fetching {name}...")
            df = self.fetch_indicator(code, countries, start_year, end_year)
            if not df.empty:
                all_data.append(df)
                print(f"  Got {len(df)} records")
        
        if all_data:
            return pd.concat(all_data, ignore_index=True)
        return pd.DataFrame()
    
    def save_to_database(self, df):
        """Save World Bank data to database."""
        if self.db is None or not self.db.is_connected():
            print("Database not connected")
            return False
        
        # Create table if not exists
        create_table_sql = """
        CREATE TABLE IF NOT EXISTS worldbank_data (
            id SERIAL PRIMARY KEY,
            country_iso3 VARCHAR(3),
            country VARCHAR(100),
            indicator_code VARCHAR(50),
            indicator_name VARCHAR(200),
            year INTEGER,
            value FLOAT,
            created_at TIMESTAMP DEFAULT NOW(),
            UNIQUE(country_iso3, indicator_code, year)
        );
        CREATE INDEX IF NOT EXISTS ix_wb_country_indicator ON worldbank_data(country_iso3, indicator_code);
        CREATE INDEX IF NOT EXISTS ix_wb_indicator_year ON worldbank_data(indicator_code, year);
        """
        
        try:
            with self.db.engine.connect() as conn:
                conn.execute(create_table_sql)
                conn.commit()
        except Exception as e:
            print(f"Table might already exist: {e}")
        
        # Insert data
        inserted = 0
        for _, row in df.iterrows():
            try:
                insert_sql = """
                INSERT INTO worldbank_data (country_iso3, country, indicator_code, indicator_name, year, value)
                VALUES (:country_iso3, :country, :indicator_code, :indicator_name, :year, :value)
                ON CONFLICT (country_iso3, indicator_code, year) DO UPDATE SET value = :value
                """
                self.db.execute_query(insert_sql, dict(row))
                inserted += 1
            except Exception as e:
                pass
        
        print(f"Inserted {inserted} records")
        return True


if __name__ == "__main__":
    # Test fetching data
    wb = WorldBankData()
    df = wb.fetch_indicator("NY.GDP.MKTP.CD", countries="USA;CHN;JPN;DEU;GBR", start_year=2010, end_year=2023)
    print(df.head(20))
