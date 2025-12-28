"""
OECD Data Loader - Using stats.oecd.org API
Fetches unique OECD indicators not covered by World Bank or IMF
"""
import requests
import psycopg2
import json
import time

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

# Our 44 countries
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
    "NOR": "Norway", "TUR": "Turkiye", "ESP": "Spain", "GBR": "United Kingdom", "IRL": "Ireland",
    "IND": "India", "CHN": "China", "JPN": "Japan", "VNM": "Vietnam", "SGP": "Singapore",
    "ISR": "Israel", "IRN": "Iran", "ARE": "United Arab Emirates", "SAU": "Saudi Arabia", "QAT": "Qatar",
    "NER": "Niger", "ZAF": "South Africa", "EGY": "Egypt", "COD": "Congo, Dem. Rep.",
    "MAR": "Morocco", "DZA": "Algeria", "ETH": "Ethiopia", "LBY": "Libya",
    "TZA": "Tanzania", "TUN": "Tunisia", "GHA": "Ghana",
    "AUS": "Australia", "NZL": "New Zealand"
}

# OECD datasets using old API format: stats.oecd.org
# Format: database/filter?startTime=YYYY
OECD_QUERIES = [
    {
        "dataset": "PDB_LV",
        "filter": "all.T_GDPHRS.CPC",  # GDP per hour worked
        "indicator_code": "OECD_LABOR_PROD",
        "indicator_name": "Labor productivity (GDP per hour worked)",
        "units": "USD, current prices, PPP",
        "category": "Labor"
    },
    {
        "dataset": "ANHRS",
        "filter": "all..",  # Average annual hours worked
        "indicator_code": "OECD_HOURS_WORKED",
        "indicator_name": "Average annual hours worked per worker",
        "units": "Hours",
        "category": "Labor"
    },
    {
        "dataset": "MSTI_PUB",
        "filter": "all.GERD_GDP",  # R&D as % of GDP
        "indicator_code": "OECD_RD_GDP",
        "indicator_name": "R&D expenditure",
        "units": "% of GDP",
        "category": "Economy"
    },
    {
        "dataset": "REV",
        "filter": "all.TOTALTAX.TAXGDP",  # Tax revenue % GDP
        "indicator_code": "OECD_TAX_GDP",
        "indicator_name": "Tax revenue (total)",
        "units": "% of GDP",
        "category": "Finance"
    },
    {
        "dataset": "HOUSE_PRICES",
        "filter": "all.RHP",  # Real house prices
        "indicator_code": "OECD_HOUSE_PRICE",
        "indicator_name": "Real house price index",
        "units": "Index (2015=100)",
        "category": "Economy"
    },
    {
        "dataset": "SHA",
        "filter": "all.HFTOT.HCTOT.PPPPER",  # Health spending per capita
        "indicator_code": "OECD_HEALTH_SPEND",
        "indicator_name": "Health spending per capita",
        "units": "USD PPP per capita",
        "category": "Health"
    },
    {
        "dataset": "PISA_2022",
        "filter": "all.READ_AVG",  # PISA reading
        "indicator_code": "OECD_PISA_READ",
        "indicator_name": "PISA reading score (15-year-olds)",
        "units": "Score",
        "category": "Education"
    },
    {
        "dataset": "PISA_2022",
        "filter": "all.MATH_AVG",  # PISA math
        "indicator_code": "OECD_PISA_MATH",
        "indicator_name": "PISA math score (15-year-olds)",
        "units": "Score",
        "category": "Education"
    },
    {
        "dataset": "PISA_2022",
        "filter": "all.SCIE_AVG",  # PISA science
        "indicator_code": "OECD_PISA_SCIE",
        "indicator_name": "PISA science score (15-year-olds)",
        "units": "Score",
        "category": "Education"
    },
]


def fetch_oecd_json(dataset, filter_str, start_year=1970):
    """Fetch data from OECD using JSON API."""
    base_url = "https://stats.oecd.org/SDMX-JSON/data"
    url = f"{base_url}/{dataset}/{filter_str}/all?startTime={start_year}"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)',
        'Accept': 'application/json'
    }
    
    try:
        print(f"    Fetching: {url[:80]}...")
        response = requests.get(url, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.HTTPError as e:
        print(f"    HTTP Error {e.response.status_code}")
        return None
    except Exception as e:
        print(f"    Error: {str(e)[:50]}")
        return None


def parse_oecd_json(data, indicator_code, indicator_name, units, category):
    """Parse OECD JSON response into records."""
    records = []
    
    if not data or 'dataSets' not in data:
        return records
    
    try:
        structure = data.get('structure', {})
        dimensions = structure.get('dimensions', {})
        
        # Get observation dimensions
        obs_dims = dimensions.get('observation', [])
        series_dims = dimensions.get('series', [])
        
        # Find country and time dimensions
        country_dim_idx = None
        time_dim_idx = None
        country_values = {}
        time_values = {}
        
        # Check series dimensions for country
        for i, dim in enumerate(series_dims):
            dim_id = dim.get('id', '')
            if dim_id in ['LOCATION', 'COU', 'COUNTRY']:
                country_dim_idx = ('series', i)
                for j, val in enumerate(dim.get('values', [])):
                    country_values[j] = val.get('id', '')
        
        # Check observation dimensions for time
        for i, dim in enumerate(obs_dims):
            dim_id = dim.get('id', '')
            if dim_id in ['TIME_PERIOD', 'TIME', 'YEAR']:
                time_dim_idx = ('obs', i)
                for j, val in enumerate(dim.get('values', [])):
                    time_values[j] = val.get('id', '')
        
        if not country_values or not time_values:
            # Try alternative parsing
            for i, dim in enumerate(series_dims):
                values = dim.get('values', [])
                if values and len(values[0].get('id', '')) == 3:
                    country_dim_idx = ('series', i)
                    for j, val in enumerate(values):
                        country_values[j] = val.get('id', '')
                    break
            
            for i, dim in enumerate(obs_dims):
                values = dim.get('values', [])
                if values and len(str(values[0].get('id', ''))) == 4:
                    time_dim_idx = ('obs', i)
                    for j, val in enumerate(values):
                        time_values[j] = val.get('id', '')
                    break
        
        # Parse data
        datasets = data.get('dataSets', [])
        if not datasets:
            return records
        
        series_data = datasets[0].get('series', {})
        
        for series_key, series_val in series_data.items():
            # Parse series key to get country
            key_parts = series_key.split(':')
            country_idx = int(key_parts[0]) if country_dim_idx and country_dim_idx[1] < len(key_parts) else None
            
            if country_idx is not None and country_idx in country_values:
                country_code = country_values[country_idx]
            else:
                continue
            
            if country_code not in COUNTRIES:
                continue
            
            country_name = COUNTRY_NAMES.get(country_code, country_code)
            
            # Get observations
            observations = series_val.get('observations', {})
            
            for time_idx_str, obs_val in observations.items():
                time_idx = int(time_idx_str)
                
                if time_idx in time_values:
                    year_str = time_values[time_idx]
                    try:
                        year = int(str(year_str)[:4])
                    except:
                        continue
                else:
                    continue
                
                # Get value (first element of observation array)
                if isinstance(obs_val, list) and len(obs_val) > 0:
                    value = obs_val[0]
                else:
                    continue
                
                if value is None:
                    continue
                
                try:
                    value = float(value)
                except:
                    continue
                
                records.append({
                    'country_iso3': country_code,
                    'country_name': country_name,
                    'indicator_code': indicator_code,
                    'indicator_name': indicator_name,
                    'units': units,
                    'category': category,
                    'year': year,
                    'value': value
                })
        
    except Exception as e:
        print(f"    Parse error: {str(e)[:50]}")
    
    return records


def main():
    print("="*60)
    print("OECD Data Loader - UNIQUE Indicators")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    print(f"\nFetching {len(OECD_QUERIES)} unique OECD indicators...")
    print("This may take several minutes...\n")
    
    total_inserted = 0
    successful = []
    
    for i, query in enumerate(OECD_QUERIES, 1):
        print(f"[{i}/{len(OECD_QUERIES)}] {query['indicator_name']}...")
        
        data = fetch_oecd_json(query['dataset'], query['filter'])
        
        if not data:
            time.sleep(2)
            continue
        
        records = parse_oecd_json(
            data,
            query['indicator_code'],
            query['indicator_name'],
            query['units'],
            query['category']
        )
        
        if not records:
            print(f"    -> No matching records")
            time.sleep(2)
            continue
        
        # Insert records
        for r in records:
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('OECD', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (r['country_iso3'], r['country_name'], r['indicator_code'],
                  r['indicator_name'], r['category'], r['year'], r['value'], r['units']))
        
        conn.commit()
        total_inserted += len(records)
        successful.append(query['indicator_name'])
        print(f"    -> {len(records)} records added")
        
        time.sleep(2)
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'OECD'")
    oecd_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'OECD'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM unified_indicators WHERE source = 'OECD'")
    countries = cur.fetchone()[0]
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    print(f"Total OECD records: {oecd_total}")
    print(f"OECD indicators: {indicators}")
    print(f"OECD countries: {countries}")
    if successful:
        print(f"\nSuccessful indicators:")
        for s in successful:
            print(f"  ✓ {s}")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
