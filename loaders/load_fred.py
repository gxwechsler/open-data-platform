"""
FRED Data Loader - Federal Reserve Economic Data (St. Louis Fed)
US-specific economic indicators

Requires API key from: https://fred.stlouisfed.org/docs/api/api_key.html
"""
import requests
import psycopg2
import time
import os

DATABASE_URL = os.environ.get("DATABASE_URL")
if not DATABASE_URL:
    raise ValueError("Set DATABASE_URL environment variable")

# FRED API key - get yours at https://fred.stlouisfed.org/docs/api/api_key.html
FRED_API_KEY = os.environ.get('FRED_API_KEY', '')

# If no env var, prompt for key
if not FRED_API_KEY:
    FRED_API_KEY = input("Enter your FRED API key: ").strip()

FRED_BASE_URL = "https://api.stlouisfed.org/fred/series/observations"

# Key FRED series to fetch (US data only)
FRED_SERIES = [
    # GDP & Output
    {
        "series_id": "GDP",
        "indicator_code": "FRED_GDP",
        "indicator_name": "Gross Domestic Product",
        "units": "Billions USD",
        "category": "Economy",
        "frequency": "Q"  # Quarterly
    },
    {
        "series_id": "GDPC1",
        "indicator_code": "FRED_REAL_GDP",
        "indicator_name": "Real Gross Domestic Product",
        "units": "Billions 2017 USD",
        "category": "Economy",
        "frequency": "Q"
    },
    # Unemployment
    {
        "series_id": "UNRATE",
        "indicator_code": "FRED_UNEMP",
        "indicator_name": "Unemployment Rate",
        "units": "%",
        "category": "Labor",
        "frequency": "M"
    },
    # Inflation
    {
        "series_id": "CPIAUCSL",
        "indicator_code": "FRED_CPI",
        "indicator_name": "Consumer Price Index (All Urban)",
        "units": "Index 1982-84=100",
        "category": "Prices",
        "frequency": "M"
    },
    {
        "series_id": "PCEPI",
        "indicator_code": "FRED_PCE",
        "indicator_name": "Personal Consumption Expenditures Price Index",
        "units": "Index 2017=100",
        "category": "Prices",
        "frequency": "M"
    },
    # Interest Rates
    {
        "series_id": "FEDFUNDS",
        "indicator_code": "FRED_FED_FUNDS",
        "indicator_name": "Federal Funds Effective Rate",
        "units": "%",
        "category": "Finance",
        "frequency": "M"
    },
    {
        "series_id": "DGS10",
        "indicator_code": "FRED_10Y_TREASURY",
        "indicator_name": "10-Year Treasury Constant Maturity Rate",
        "units": "%",
        "category": "Finance",
        "frequency": "D"
    },
    {
        "series_id": "DGS2",
        "indicator_code": "FRED_2Y_TREASURY",
        "indicator_name": "2-Year Treasury Constant Maturity Rate",
        "units": "%",
        "category": "Finance",
        "frequency": "D"
    },
    # Money Supply
    {
        "series_id": "M2SL",
        "indicator_code": "FRED_M2",
        "indicator_name": "M2 Money Supply",
        "units": "Billions USD",
        "category": "Finance",
        "frequency": "M"
    },
    # Housing
    {
        "series_id": "HOUST",
        "indicator_code": "FRED_HOUSING_STARTS",
        "indicator_name": "Housing Starts",
        "units": "Thousands of Units",
        "category": "Economy",
        "frequency": "M"
    },
    {
        "series_id": "CSUSHPISA",
        "indicator_code": "FRED_HOUSE_PRICE",
        "indicator_name": "S&P/Case-Shiller Home Price Index",
        "units": "Index Jan 2000=100",
        "category": "Economy",
        "frequency": "M"
    },
    # Industrial Production
    {
        "series_id": "INDPRO",
        "indicator_code": "FRED_IND_PROD",
        "indicator_name": "Industrial Production Index",
        "units": "Index 2017=100",
        "category": "Economy",
        "frequency": "M"
    },
    # Consumer Sentiment
    {
        "series_id": "UMCSENT",
        "indicator_code": "FRED_CONSUMER_SENT",
        "indicator_name": "Consumer Sentiment (U of Michigan)",
        "units": "Index 1966=100",
        "category": "Economy",
        "frequency": "M"
    },
    # Trade
    {
        "series_id": "BOPGSTB",
        "indicator_code": "FRED_TRADE_BALANCE",
        "indicator_name": "Trade Balance (Goods & Services)",
        "units": "Millions USD",
        "category": "Economy",
        "frequency": "M"
    },
    # Government Debt
    {
        "series_id": "GFDEBTN",
        "indicator_code": "FRED_FED_DEBT",
        "indicator_name": "Federal Debt Total Public Debt",
        "units": "Millions USD",
        "category": "Finance",
        "frequency": "Q"
    },
]


def fetch_fred_series(series_id, start_date="1950-01-01"):
    """Fetch data from FRED API."""
    params = {
        "series_id": series_id,
        "api_key": FRED_API_KEY,
        "file_type": "json",
        "observation_start": start_date
    }
    
    try:
        response = requests.get(FRED_BASE_URL, params=params, timeout=30)
        response.raise_for_status()
        data = response.json()
        return data.get('observations', [])
    except Exception as e:
        print(f"    Error: {e}")
        return []


def aggregate_to_annual(observations, frequency):
    """Aggregate monthly/quarterly/daily data to annual averages."""
    from collections import defaultdict
    
    year_values = defaultdict(list)
    
    for obs in observations:
        date_str = obs.get('date', '')
        value_str = obs.get('value', '')
        
        if not date_str or value_str == '.':
            continue
        
        try:
            year = int(date_str[:4])
            value = float(value_str)
            year_values[year].append(value)
        except (ValueError, TypeError):
            continue
    
    # Calculate annual averages
    annual_data = {}
    for year, values in year_values.items():
        if values:
            annual_data[year] = sum(values) / len(values)
    
    return annual_data


def main():
    print("="*60)
    print("FRED Data Loader - Federal Reserve Economic Data")
    print("="*60)
    
    if not FRED_API_KEY:
        print("\nERROR: No FRED API key provided.")
        print("Get a free key at: https://fred.stlouisfed.org/docs/api/api_key.html")
        print("Then run: FRED_API_KEY=your_key python3 load_fred.py")
        return
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    print(f"\nFetching {len(FRED_SERIES)} FRED indicators (US data only)...")
    
    total_inserted = 0
    
    for i, series in enumerate(FRED_SERIES, 1):
        series_id = series['series_id']
        indicator_code = series['indicator_code']
        indicator_name = series['indicator_name']
        
        print(f"[{i}/{len(FRED_SERIES)}] {indicator_name}...")
        
        observations = fetch_fred_series(series_id)
        
        if not observations:
            print(f"    -> No data")
            time.sleep(0.5)
            continue
        
        # Aggregate to annual
        annual_data = aggregate_to_annual(observations, series['frequency'])
        
        if not annual_data:
            print(f"    -> No annual data")
            time.sleep(0.5)
            continue
        
        # Insert records (US only)
        records_count = 0
        for year, value in annual_data.items():
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('FRED', 'USA', 'United States', %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (indicator_code, indicator_name, series['category'], 
                  year, value, series['units']))
            records_count += 1
        
        conn.commit()
        total_inserted += records_count
        print(f"    -> {records_count} records ({min(annual_data.keys())}-{max(annual_data.keys())})")
        
        time.sleep(0.5)  # Rate limiting
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'FRED'")
    fred_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'FRED'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators WHERE source = 'FRED'")
    years = cur.fetchone()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    print(f"Total FRED records: {fred_total}")
    print(f"FRED indicators: {indicators}")
    print(f"FRED country: USA only")
    if years[0]:
        print(f"FRED year range: {years[0]} - {years[1]}")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
