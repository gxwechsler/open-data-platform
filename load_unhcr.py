"""
UNHCR Data Loader - UN Refugee Agency Statistics
Refugee populations, asylum applications, and IDPs by country

API: https://api.unhcr.org/population/v1/
No API key required
"""
import requests
import psycopg2
import time
from collections import defaultdict

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

# Our 44 countries (ISO3 codes)
COUNTRIES = [
    "ARG", "BRA", "CHL", "COL", "MEX", "USA", "CAN",
    "DEU", "FRA", "ITA", "SWE", "NLD", "CHE", "DNK", "FIN", "NOR", "TUR", "ESP", "GBR", "IRL",
    "IND", "CHN", "JPN", "VNM", "SGP",
    "ISR", "IRN", "ARE", "SAU", "QAT",
    "NER", "ZAF", "EGY", "COD", "MAR", "DZA", "ETH", "LBY", "TZA", "TUN", "GHA",
    "AUS", "NZL"
]

COUNTRY_NAMES = {
    "ARG": "Argentina", "BRA": "Brazil", "CHL": "Chile", "COL": "Colombia",
    "MEX": "Mexico", "USA": "United States", "CAN": "Canada",
    "DEU": "Germany", "FRA": "France", "ITA": "Italy", "SWE": "Sweden",
    "NLD": "Netherlands", "CHE": "Switzerland", "DNK": "Denmark", "FIN": "Finland",
    "NOR": "Norway", "TUR": "Turkey", "ESP": "Spain", "GBR": "United Kingdom", "IRL": "Ireland",
    "IND": "India", "CHN": "China", "JPN": "Japan", "VNM": "Vietnam", "SGP": "Singapore",
    "ISR": "Israel", "IRN": "Iran", "ARE": "United Arab Emirates", "SAU": "Saudi Arabia", "QAT": "Qatar",
    "NER": "Niger", "ZAF": "South Africa", "EGY": "Egypt", "COD": "Congo, Dem. Rep.",
    "MAR": "Morocco", "DZA": "Algeria", "ETH": "Ethiopia", "LBY": "Libya",
    "TZA": "Tanzania", "TUN": "Tunisia", "GHA": "Ghana",
    "AUS": "Australia", "NZL": "New Zealand"
}

API_BASE = "https://api.unhcr.org/population/v1"


def safe_int(val):
    """Safely convert a value to int, handling None, strings, etc."""
    if val is None:
        return 0
    try:
        return int(float(val))
    except (ValueError, TypeError):
        return 0


def fetch_population_data(coa=None, coo=None, year_from=2000, year_to=2024):
    """
    Fetch refugee population data.
    coa = country of asylum (where refugees are hosted)
    coo = country of origin (where refugees come from)
    """
    params = {
        "limit": 10000,
        "yearFrom": year_from,
        "yearTo": year_to,
    }
    
    if coa:
        params["coa"] = coa
    if coo:
        params["coo"] = coo
    
    try:
        response = requests.get(f"{API_BASE}/population/", params=params, timeout=60)
        response.raise_for_status()
        data = response.json()
        return data.get('items', [])
    except Exception as e:
        print(f"    Error: {e}")
        return []


def main():
    print("="*60)
    print("UNHCR Data Loader - Refugee Statistics")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    total_inserted = 0
    
    # =============================================================================
    # 1. REFUGEES HOSTED (Country of Asylum)
    # =============================================================================
    print("\n[1/4] Fetching refugees hosted by country...")
    
    refugees_hosted = defaultdict(lambda: defaultdict(int))
    
    for iso3 in COUNTRIES:
        data = fetch_population_data(coa=iso3)
        time.sleep(0.3)
        
        for item in data:
            year = safe_int(item.get('year'))
            refugees = safe_int(item.get('refugees'))
            ref_assisted = safe_int(item.get('refugeesAssistedByUnhcr'))
            
            if year:
                refugees_hosted[iso3][year] += refugees + ref_assisted
    
    records_count = 0
    for iso3, year_data in refugees_hosted.items():
        for year, count in year_data.items():
            if count > 0:
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UNHCR', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, COUNTRY_NAMES[iso3], 'UNHCR_REFUGEES_HOSTED',
                      'Refugees hosted', 'Population', int(year), count, 'Persons'))
                records_count += 1
    
    conn.commit()
    total_inserted += records_count
    print(f"    -> {records_count} records")
    
    # =============================================================================
    # 2. REFUGEES FROM (Country of Origin)
    # =============================================================================
    print("\n[2/4] Fetching refugees from country (origin)...")
    
    refugees_from = defaultdict(lambda: defaultdict(int))
    
    for iso3 in COUNTRIES:
        data = fetch_population_data(coo=iso3)
        time.sleep(0.3)
        
        for item in data:
            year = safe_int(item.get('year'))
            refugees = safe_int(item.get('refugees'))
            ref_assisted = safe_int(item.get('refugeesAssistedByUnhcr'))
            
            if year:
                refugees_from[iso3][year] += refugees + ref_assisted
    
    records_count = 0
    for iso3, year_data in refugees_from.items():
        for year, count in year_data.items():
            if count > 0:
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UNHCR', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, COUNTRY_NAMES[iso3], 'UNHCR_REFUGEES_ORIGIN',
                      'Refugees from country (origin)', 'Population', int(year), count, 'Persons'))
                records_count += 1
    
    conn.commit()
    total_inserted += records_count
    print(f"    -> {records_count} records")
    
    # =============================================================================
    # 3. INTERNALLY DISPLACED PERSONS (IDPs)
    # =============================================================================
    print("\n[3/4] Fetching IDPs...")
    
    idps = defaultdict(lambda: defaultdict(int))
    
    for iso3 in COUNTRIES:
        data = fetch_population_data(coo=iso3)
        time.sleep(0.3)
        
        for item in data:
            year = safe_int(item.get('year'))
            idp_count = safe_int(item.get('idps'))
            
            if year and idp_count > 0:
                # Only count IDPs for origin country
                if item.get('coo') == iso3:
                    idps[iso3][year] += idp_count
    
    records_count = 0
    for iso3, year_data in idps.items():
        for year, count in year_data.items():
            if count > 0:
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UNHCR', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, COUNTRY_NAMES[iso3], 'UNHCR_IDPS',
                      'Internally displaced persons', 'Population', int(year), count, 'Persons'))
                records_count += 1
    
    conn.commit()
    total_inserted += records_count
    print(f"    -> {records_count} records")
    
    # =============================================================================
    # 4. ASYLUM SEEKERS
    # =============================================================================
    print("\n[4/4] Fetching asylum seekers...")
    
    asylum_seekers = defaultdict(lambda: defaultdict(int))
    
    for iso3 in COUNTRIES:
        data = fetch_population_data(coa=iso3)
        
        for item in data:
            year = safe_int(item.get('year'))
            seekers = safe_int(item.get('asylumSeekers'))
            
            if year and seekers > 0:
                asylum_seekers[iso3][year] += seekers
    
    records_count = 0
    for iso3, year_data in asylum_seekers.items():
        for year, count in year_data.items():
            if count > 0:
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UNHCR', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, COUNTRY_NAMES[iso3], 'UNHCR_ASYLUM_SEEKERS',
                      'Asylum seekers', 'Population', int(year), count, 'Persons'))
                records_count += 1
    
    conn.commit()
    total_inserted += records_count
    print(f"    -> {records_count} records")
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'UNHCR'")
    unhcr_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'UNHCR'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM unified_indicators WHERE source = 'UNHCR'")
    countries = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators WHERE source = 'UNHCR'")
    years = cur.fetchone()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    print(f"Total UNHCR records: {unhcr_total}")
    print(f"UNHCR indicators: {indicators}")
    print(f"UNHCR countries: {countries}")
    if years[0]:
        print(f"UNHCR year range: {years[0]} - {years[1]}")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
