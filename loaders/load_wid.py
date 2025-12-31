"""Load WID (World Inequality Database) data into Supabase."""
import pandas as pd
import os
import sys
from pathlib import Path
from sqlalchemy import text

sys.path.insert(0, str(Path(__file__).parent))
from database.connection import get_db_manager

# ISO3 to ISO2 mapping for your 43 countries
ISO3_TO_ISO2 = {
    'ARE': 'AE', 'ARG': 'AR', 'AUS': 'AU', 'BRA': 'BR', 'CAN': 'CA',
    'CHE': 'CH', 'CHL': 'CL', 'CHN': 'CN', 'COD': 'CD', 'COL': 'CO',
    'DEU': 'DE', 'DNK': 'DK', 'DZA': 'DZ', 'EGY': 'EG', 'ESP': 'ES',
    'ETH': 'ET', 'FIN': 'FI', 'FRA': 'FR', 'GBR': 'GB', 'GHA': 'GH',
    'IND': 'IN', 'IRL': 'IE', 'IRN': 'IR', 'ISR': 'IL', 'ITA': 'IT',
    'JPN': 'JP', 'LBY': 'LY', 'MAR': 'MA', 'MEX': 'MX', 'NER': 'NE',
    'NLD': 'NL', 'NOR': 'NO', 'NZL': 'NZ', 'QAT': 'QA', 'SAU': 'SA',
    'SGP': 'SG', 'SWE': 'SE', 'TUN': 'TN', 'TUR': 'TR', 'TZA': 'TZ',
    'USA': 'US', 'VNM': 'VN', 'ZAF': 'ZA'
}
ISO2_TO_ISO3 = {v: k for k, v in ISO3_TO_ISO2.items()}

COUNTRY_NAMES = {}

INDICATORS = {
    'sptincj992': 'Pre-tax National Income Share',
    'sptincj999': 'Pre-tax National Income Share',
    'sdiincj992': 'Post-tax Disposable Income Share',
    'sdiincj999': 'Post-tax Disposable Income Share',
    'shwealj992': 'Net Personal Wealth Share',
    'shwealj999': 'Net Personal Wealth Share',
    'gptincj992': 'Gini Pre-tax National Income',
    'gptincj999': 'Gini Pre-tax National Income',
    'gdiincj992': 'Gini Post-tax Disposable Income',
    'gdiincj999': 'Gini Post-tax Disposable Income',
    'ghwealj992': 'Gini Net Personal Wealth',
    'ghwealj999': 'Gini Net Personal Wealth',
}

PERCENTILES = ['p0p50', 'p50p90', 'p90p100', 'p99p100', 'p0p100']

WID_DATA_DIR = Path('data/wid')

def load_country_names():
    global COUNTRY_NAMES
    path = WID_DATA_DIR / 'WID_countries.csv'
    df = pd.read_csv(path, sep=';')
    COUNTRY_NAMES = dict(zip(df['alpha2'], df['shortname']))
    print(f"Loaded {len(COUNTRY_NAMES)} country names")

def build_indicator_code(variable, percentile):
    return f"WID_{variable}_{percentile}"

def build_indicator_name(variable, percentile):
    base = INDICATORS.get(variable, variable)
    pct_labels = {
        'p0p50': 'Bottom 50%',
        'p50p90': 'Middle 40%',
        'p90p100': 'Top 10%',
        'p99p100': 'Top 1%',
        'p0p100': 'Total Population'
    }
    pct = pct_labels.get(percentile, percentile)
    return f"{base} - {pct}"

def get_units(variable):
    if variable.startswith('g'):
        return 'index (0-1)'
    elif variable.startswith('s'):
        return 'share (0-1)'
    return None

def load_country_data(iso2):
    path = WID_DATA_DIR / f'WID_data_{iso2}.csv'
    if not path.exists():
        print(f"  File not found: {path}")
        return pd.DataFrame()
    
    df = pd.read_csv(path, sep=';')
    df = df[df['variable'].isin(INDICATORS.keys())]
    df = df[df['percentile'].isin(PERCENTILES)]
    df = df[df['year'] >= 1950]
    return df

def transform_to_unified(df, iso2):
    if df.empty:
        return []
    
    iso3 = ISO2_TO_ISO3.get(iso2)
    country_name = COUNTRY_NAMES.get(iso2, iso2)
    
    records = []
    for _, row in df.iterrows():
        variable = row['variable']
        percentile = row['percentile']
        
        record = {
            'indicator_code': build_indicator_code(variable, percentile),
            'indicator_name': build_indicator_name(variable, percentile),
            'country_iso3': iso3,
            'country_name': country_name,
            'year': int(row['year']),
            'value': float(row['value']) if pd.notna(row['value']) else None,
            'units': get_units(variable),
            'source': 'WID',
            'category': 'inequality'
        }
        records.append(record)
    
    return records

def load_world_data():
    path = WID_DATA_DIR / 'WID_data_WO.csv'
    if not path.exists():
        print(f"  World file not found: {path}")
        return []
    
    df = pd.read_csv(path, sep=';')
    world_indicators = ['sptincj999', 'sptincj992', 'shwealj999', 'shwealj992']
    df = df[df['variable'].isin(world_indicators)]
    df = df[df['percentile'].isin(PERCENTILES)]
    df = df[df['year'] >= 1950]
    
    records = []
    for _, row in df.iterrows():
        variable = row['variable']
        percentile = row['percentile']
        
        record = {
            'indicator_code': build_indicator_code(variable, percentile) + '_GLOBAL',
            'indicator_name': 'Global ' + build_indicator_name(variable, percentile),
            'country_iso3': 'WLD',
            'country_name': 'World',
            'year': int(row['year']),
            'value': float(row['value']) if pd.notna(row['value']) else None,
            'units': get_units(variable),
            'source': 'WID',
            'category': 'inequality'
        }
        records.append(record)
    
    return records

def insert_records_batch(engine, records, batch_size=500):
    """Insert records with proper commit"""
    total = len(records)
    inserted = 0
    
    for i in range(0, total, batch_size):
        batch = records[i:i+batch_size]
        
        with engine.connect() as conn:
            for record in batch:
                query = text("""
                    INSERT INTO time_series_unified_data 
                    (indicator_code, indicator_name, country_iso3, country_name, year, value, units, source, category)
                    VALUES (:indicator_code, :indicator_name, :country_iso3, :country_name, :year, :value, :units, :source, :category)
                """)
                conn.execute(query, record)
            conn.commit()
        
        inserted += len(batch)
        print(f"  Inserted {inserted}/{total} records")
    
    return inserted

def main():
    print("=== WID Data Loader ===\n")
    
    load_country_names()
    
    db = get_db_manager()
    engine = db.engine
    
    # Check for existing WID data
    existing = db.execute_query("SELECT COUNT(*) as cnt FROM time_series_unified_data WHERE source = 'WID'")
    if existing and existing[0]['cnt'] > 0:
        print(f"Found {existing[0]['cnt']} existing WID records.")
        resp = input("Delete existing WID data before loading? (y/n): ")
        if resp.lower() == 'y':
            with engine.connect() as conn:
                conn.execute(text("DELETE FROM time_series_unified_data WHERE source = 'WID'"))
                conn.commit()
            print("Deleted existing WID data.\n")
        else:
            print("Aborting to prevent duplicates.\n")
            return
    
    all_records = []
    
    print("\n--- Loading country data ---")
    for iso3, iso2 in sorted(ISO3_TO_ISO2.items()):
        print(f"Processing {iso3} ({iso2})...")
        df = load_country_data(iso2)
        if not df.empty:
            records = transform_to_unified(df, iso2)
            all_records.extend(records)
            print(f"  {len(records)} records")
        else:
            print(f"  No data found")
    
    print("\n--- Loading global inequality data ---")
    world_records = load_world_data()
    all_records.extend(world_records)
    print(f"  {len(world_records)} global records")
    
    print(f"\n--- Inserting {len(all_records)} total records ---")
    if all_records:
        inserted = insert_records_batch(engine, all_records)
        print(f"\n=== Done! Inserted {inserted} records ===")
    else:
        print("No records to insert.")

if __name__ == '__main__':
    main()
