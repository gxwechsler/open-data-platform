"""
UCDP Data Loader - Fetches armed conflict data from Uppsala Conflict Data Program API
Data: Battle deaths, conflict events by country and year
"""
import requests
import psycopg2
import time
from collections import defaultdict

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL environment variable")

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

# UCDP uses country names, map them to ISO3
UCDP_COUNTRY_MAP = {
    "Argentina": "ARG", "Brazil": "BRA", "Chile": "CHL", "Colombia": "COL",
    "Mexico": "MEX", "United States of America": "USA", "Canada": "CAN",
    "Germany": "DEU", "Germany, Federal Republic of": "DEU", "France": "FRA", 
    "Italy": "ITA", "Sweden": "SWE", "Netherlands": "NLD", "Switzerland": "CHE", 
    "Denmark": "DNK", "Finland": "FIN", "Norway": "NOR", "Turkey": "TUR", 
    "Spain": "ESP", "United Kingdom": "GBR", "Ireland": "IRL",
    "India": "IND", "China": "CHN", "Japan": "JPN", "Vietnam": "VNM",
    "Vietnam, Republic of": "VNM", "Vietnam (North Vietnam)": "VNM", 
    "Singapore": "SGP", "Israel": "ISR", "Iran": "IRN", 
    "United Arab Emirates": "ARE", "Saudi Arabia": "SAU", "Qatar": "QAT",
    "Niger": "NER", "South Africa": "ZAF", "Egypt": "EGY", 
    "Congo, Democratic Republic of (Zaire)": "COD", "DR Congo (Zaire)": "COD",
    "Morocco": "MAR", "Algeria": "DZA", "Ethiopia": "ETH", "Libya": "LBY",
    "Tanzania": "TZA", "Tunisia": "TUN", "Ghana": "GHA",
    "Australia": "AUS", "New Zealand": "NZL"
}

API_BASE = "https://ucdpapi.pcr.uu.se/api"
VERSION = "25.1"


def fetch_all_pages(endpoint, params=None):
    """Fetch all pages from UCDP API."""
    all_results = []
    page = 0
    pagesize = 1000
    
    while True:
        url = f"{API_BASE}/{endpoint}/{VERSION}"
        request_params = {"pagesize": pagesize, "page": page}
        if params:
            request_params.update(params)
        
        try:
            response = requests.get(url, params=request_params, timeout=30)
            response.raise_for_status()
            data = response.json()
            
            results = data.get('Result', [])
            all_results.extend(results)
            
            total_pages = data.get('TotalPages', 1)
            print(f"    Page {page + 1}/{total_pages} ({len(results)} records)")
            
            if page >= total_pages - 1:
                break
            
            page += 1
            time.sleep(0.5)  # Rate limiting
            
        except Exception as e:
            print(f"    Error: {e}")
            break
    
    return all_results


def process_battle_deaths(data):
    """Process battle deaths data into country-year records."""
    # Aggregate by country and year
    country_year_deaths = defaultdict(lambda: defaultdict(float))
    
    for record in data:
        location = record.get('location', '')
        year = record.get('year')
        bd_best = record.get('bd_best', 0) or 0  # Best estimate of deaths
        
        if not year:
            continue
        
        # Handle multiple locations (comma-separated)
        locations = [loc.strip() for loc in location.split(',')]
        
        for loc in locations:
            iso3 = UCDP_COUNTRY_MAP.get(loc)
            if iso3 and iso3 in COUNTRIES:
                # Divide deaths among countries if multiple
                country_year_deaths[iso3][year] += bd_best / len(locations)
    
    return country_year_deaths


def process_conflicts(data):
    """Process conflict data into country-year records."""
    # Count conflicts by country and year
    country_year_conflicts = defaultdict(lambda: defaultdict(int))
    
    for record in data:
        location = record.get('location', '')
        year = record.get('year')
        
        if not year:
            continue
        
        locations = [loc.strip() for loc in location.split(',')]
        
        for loc in locations:
            iso3 = UCDP_COUNTRY_MAP.get(loc)
            if iso3 and iso3 in COUNTRIES:
                country_year_conflicts[iso3][year] += 1
    
    return country_year_conflicts


def process_one_sided_violence(data):
    """Process one-sided violence data (civilian casualties)."""
    country_year_onesided = defaultdict(lambda: defaultdict(float))
    
    for record in data:
        location = record.get('location', '')
        year = record.get('year')
        best = record.get('best_fatality_estimate', 0) or 0
        
        if not year:
            continue
        
        locations = [loc.strip() for loc in location.split(',')]
        
        for loc in locations:
            iso3 = UCDP_COUNTRY_MAP.get(loc)
            if iso3 and iso3 in COUNTRIES:
                country_year_onesided[iso3][year] += best / len(locations)
    
    return country_year_onesided


def main():
    print("="*60)
    print("UCDP Data Loader - Armed Conflict Statistics")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    total_inserted = 0
    
    # 1. Battle Deaths Dataset
    print("\n[1/3] Fetching Battle Deaths data...")
    bd_data = fetch_all_pages("battledeaths")
    
    if bd_data:
        deaths_by_country = process_battle_deaths(bd_data)
        records_count = 0
        
        for iso3, year_data in deaths_by_country.items():
            country_name = COUNTRY_NAMES.get(iso3, iso3)
            for year, deaths in year_data.items():
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UCDP', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, country_name, 'UCDP_BATTLE_DEATHS', 
                      'Battle-related deaths', 'Security', int(year), deaths, 'Deaths'))
                records_count += 1
        
        conn.commit()
        total_inserted += records_count
        print(f"    -> {records_count} records added")
    
    time.sleep(2)
    
    # 2. Armed Conflicts Dataset  
    print("\n[2/3] Fetching Armed Conflicts data...")
    conflict_data = fetch_all_pages("ucdpprioconflict")
    
    if conflict_data:
        conflicts_by_country = process_conflicts(conflict_data)
        records_count = 0
        
        for iso3, year_data in conflicts_by_country.items():
            country_name = COUNTRY_NAMES.get(iso3, iso3)
            for year, count in year_data.items():
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UCDP', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, country_name, 'UCDP_CONFLICTS',
                      'Active armed conflicts', 'Security', int(year), count, 'Count'))
                records_count += 1
        
        conn.commit()
        total_inserted += records_count
        print(f"    -> {records_count} records added")
    
    time.sleep(2)
    
    # 3. One-Sided Violence (civilian deaths)
    print("\n[3/3] Fetching One-Sided Violence data...")
    onesided_data = fetch_all_pages("onesided")
    
    if onesided_data:
        onesided_by_country = process_one_sided_violence(onesided_data)
        records_count = 0
        
        for iso3, year_data in onesided_by_country.items():
            country_name = COUNTRY_NAMES.get(iso3, iso3)
            for year, deaths in year_data.items():
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('UCDP', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, country_name, 'UCDP_ONESIDED',
                      'One-sided violence deaths (civilians)', 'Security', int(year), deaths, 'Deaths'))
                records_count += 1
        
        conn.commit()
        total_inserted += records_count
        print(f"    -> {records_count} records added")
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'UCDP'")
    ucdp_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'UCDP'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM unified_indicators WHERE source = 'UCDP'")
    countries = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators WHERE source = 'UCDP'")
    years = cur.fetchone()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    print(f"Total UCDP records: {ucdp_total}")
    print(f"UCDP indicators: {indicators}")
    print(f"UCDP countries: {countries}")
    if years[0]:
        print(f"UCDP year range: {years[0]} - {years[1]}")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
