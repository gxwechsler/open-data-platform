"""
IMF Data Loader - Fetches data from IMF DataMapper API
and loads into unified_indicators table.
"""
import requests
import psycopg2
import time

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL environment variable")

# Our 44 countries (ISO3 codes)
COUNTRIES = [
    "ARG", "BRA", "CHL", "COL", "MEX", "USA", "CAN",  # Americas
    "DEU", "FRA", "ITA", "SWE", "NLD", "CHE", "DNK", "FIN", "NOR", "TUR", "ESP", "GBR", "IRL",  # Europe
    "IND", "CHN", "JPN", "VNM", "SGP",  # Asia
    "ISR", "IRN", "ARE", "SAU", "QAT",  # Middle East
    "NER", "ZAF", "EGY", "COD", "MAR", "DZA", "ETH", "LBY", "TZA", "TUN", "GHA",  # Africa
    "AUS", "NZL"  # South Pacific
]

# IMF DataMapper indicators to fetch
INDICATORS = {
    "NGDPD": ("GDP, current prices", "Billions of U.S. dollars", "Economy"),
    "NGDPDPC": ("GDP per capita, current prices", "U.S. dollars", "Economy"),
    "NGDP_RPCH": ("GDP growth rate", "Annual percent change", "Economy"),
    "PPPPC": ("GDP per capita, PPP", "Current international dollar", "Economy"),
    "PCPIPCH": ("Inflation rate, average consumer prices", "Annual percent change", "Prices"),
    "PCPIEPCH": ("Inflation rate, end of period", "Annual percent change", "Prices"),
    "LUR": ("Unemployment rate", "Percent of total labor force", "Labor"),
    "LP": ("Population", "Millions", "Population"),
    "BCA": ("Current account balance", "Percent of GDP", "Finance"),
    "BCA_NGDPD": ("Current account balance", "Billions of U.S. dollars", "Finance"),
    "GGXWDG_NGDP": ("General government gross debt", "Percent of GDP", "Finance"),
    "GGXCNL_NGDP": ("General government net lending/borrowing", "Percent of GDP", "Finance"),
    "GGR_NGDP": ("General government revenue", "Percent of GDP", "Finance"),
    "GGX_NGDP": ("General government total expenditure", "Percent of GDP", "Finance"),
    "NGSD_NGDP": ("Gross national savings", "Percent of GDP", "Finance"),
    "NID_NGDP": ("Total investment", "Percent of GDP", "Finance"),
    "TM_RPCH": ("Volume of imports of goods and services", "Annual percent change", "Economy"),
    "TX_RPCH": ("Volume of exports of goods and services", "Annual percent change", "Economy"),
}

# Country name mapping (ISO3 to full name)
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


def fetch_imf_indicator(indicator_code):
    """Fetch data for a single indicator from IMF DataMapper API."""
    url = f"https://www.imf.org/external/datamapper/api/v1/{indicator_code}"
    
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        data = response.json()
        
        if "values" not in data or indicator_code not in data["values"]:
            return {}
        
        return data["values"][indicator_code]
    
    except Exception as e:
        print(f"  Error fetching {indicator_code}: {e}")
        return {}


def main():
    print("="*60)
    print("IMF Data Loader")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    print(f"\nFetching {len(INDICATORS)} indicators for {len(COUNTRIES)} countries...")
    print("This will take a few minutes...\n")
    
    total_inserted = 0
    
    for i, (code, (name, units, category)) in enumerate(INDICATORS.items(), 1):
        print(f"[{i}/{len(INDICATORS)}] {name}...")
        
        indicator_data = fetch_imf_indicator(code)
        
        if not indicator_data:
            print(f"    -> No data")
            time.sleep(0.5)
            continue
        
        records_count = 0
        
        for country_iso3 in COUNTRIES:
            if country_iso3 not in indicator_data:
                continue
            
            country_name = COUNTRY_NAMES.get(country_iso3, country_iso3)
            country_values = indicator_data[country_iso3]
            
            for year_str, value in country_values.items():
                if value is None:
                    continue
                
                try:
                    year = int(year_str)
                    value = float(value)
                    
                    cur.execute("""
                        INSERT INTO unified_indicators 
                        (source, country_iso3, country_name, indicator_code, indicator_name, 
                         category, year, value, units)
                        VALUES ('IMF', %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, country_iso3, indicator_code, year) 
                        DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                    """, (country_iso3, country_name, code, name, category, year, value, units))
                    
                    records_count += 1
                    
                except (ValueError, TypeError):
                    continue
        
        conn.commit()
        total_inserted += records_count
        print(f"    -> {records_count} records")
        
        time.sleep(0.5)  # Rate limiting
    
    # Get final counts
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'IMF'")
    imf_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'IMF'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM unified_indicators WHERE source = 'IMF'")
    countries = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators WHERE source = 'IMF'")
    years = cur.fetchone()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"IMF records added: {total_inserted}")
    print(f"Total IMF records: {imf_total}")
    print(f"IMF indicators: {indicators}")
    print(f"IMF countries: {countries}")
    print(f"IMF year range: {years[0]} - {years[1]}")
    print(f"\nTotal records in database: {total}")
    print("="*60)
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
