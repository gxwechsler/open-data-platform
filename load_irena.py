"""
IRENA Data Loader - Fetches renewable energy data from IRENA PxWeb API
FIXED: IRENA uses ISO3 codes directly (ARG, BRA, USA, etc.)
"""
import requests
import psycopg2
import json
import time

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

PXWEB_BASE = "https://pxweb.irena.org/api/v1/en/IRENASTAT"


def get_table_metadata(table_path):
    """Get metadata about table dimensions."""
    url = f"{PXWEB_BASE}/{table_path}"
    try:
        response = requests.get(url, timeout=30)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  Metadata error: {e}")
        return None


def query_pxweb_table(table_path, query):
    """Query PxWeb table with POST request."""
    url = f"{PXWEB_BASE}/{table_path}"
    headers = {"Content-Type": "application/json"}
    
    try:
        response = requests.post(url, json=query, headers=headers, timeout=60)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        print(f"  Query error: {e}")
        return None


def parse_jsonstat2(data, indicator_code, indicator_name, units, category):
    """Parse JSON-stat2 response into records."""
    records = []
    
    if not data:
        return records
    
    try:
        dimensions = data.get('dimension', {})
        values = data.get('value', [])
        dim_ids = data.get('id', [])
        dim_sizes = data.get('size', [])
        
        if not dimensions or not values:
            return records
        
        # Find country and year dimension indices
        country_dim_idx = None
        year_dim_idx = None
        indicator_dim_idx = None
        
        country_values = {}
        year_values = {}
        indicator_values = {}
        
        for i, dim_id in enumerate(dim_ids):
            dim = dimensions.get(dim_id, {})
            label = dim.get('label', '').lower()
            cat = dim.get('category', {})
            idx_map = cat.get('index', {})
            label_map = cat.get('label', {})
            
            if 'country' in label or 'region' in label or 'area' in label:
                country_dim_idx = i
                country_values = {v: k for k, v in idx_map.items()}  # idx -> code
            elif 'year' in label:
                year_dim_idx = i
                year_values = {v: k for k, v in idx_map.items()}
            elif 'indicator' in label:
                indicator_dim_idx = i
                indicator_values = {v: label_map.get(k, k) for k, v in idx_map.items()}
        
        if country_dim_idx is None or year_dim_idx is None:
            print(f"  Missing dimensions: country={country_dim_idx}, year={year_dim_idx}")
            return records
        
        # Parse each value
        for flat_idx, value in enumerate(values):
            if value is None:
                continue
            
            # Convert flat index to multi-dimensional indices
            indices = []
            remaining = flat_idx
            for size in reversed(dim_sizes):
                indices.insert(0, remaining % size)
                remaining //= size
            
            # Get country code (IRENA uses ISO3!)
            country_idx = indices[country_dim_idx]
            country_code = country_values.get(country_idx, '')
            
            # Check if it's one of our countries
            if country_code not in COUNTRIES:
                continue
            
            country_name = COUNTRY_NAMES.get(country_code, country_code)
            
            # Get year
            year_idx = indices[year_dim_idx]
            year_str = year_values.get(year_idx, '')
            try:
                year = int(year_str)
            except:
                continue
            
            # Get indicator name if multiple
            ind_name = indicator_name
            if indicator_dim_idx is not None:
                ind_idx = indices[indicator_dim_idx]
                ind_label = indicator_values.get(ind_idx, '')
                if 'capacity' in ind_label.lower():
                    ind_name = "Renewable share of electricity capacity"
                    ind_code = "IRENA_RE_CAP_SHARE"
                elif 'generation' in ind_label.lower():
                    ind_name = "Renewable share of electricity generation"
                    ind_code = "IRENA_RE_GEN_SHARE"
                else:
                    ind_code = indicator_code
            else:
                ind_code = indicator_code
            
            records.append({
                'country_iso3': country_code,
                'country_name': country_name,
                'indicator_code': ind_code,
                'indicator_name': ind_name,
                'units': units,
                'category': category,
                'year': year,
                'value': float(value)
            })
    
    except Exception as e:
        print(f"  Parse error: {e}")
    
    return records


def main():
    print("="*60)
    print("IRENA Data Loader - Renewable Energy Statistics (FIXED)")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    # First, clear old IRENA data (to avoid duplicates from bad run)
    print("\nClearing old IRENA data...")
    cur.execute("DELETE FROM unified_indicators WHERE source = 'IRENA'")
    conn.commit()
    
    total_inserted = 0
    
    # === 1. Renewable Share Data ===
    print("\n[1/2] Fetching Renewable Energy Share data...")
    table_path = "Power%20Capacity%20and%20Generation/RE-SHARE_2025_H2_PX.px"
    
    meta = get_table_metadata(table_path)
    if meta:
        variables = meta.get('variables', [])
        query_items = []
        
        for var in variables:
            code = var.get('code', '')
            vals = var.get('values', [])
            text = var.get('text', '').lower()
            
            if 'country' in text or 'region' in text or 'area' in text:
                # Select our 44 countries
                our_countries = [v for v in vals if v in COUNTRIES]
                print(f"  Found {len(our_countries)} of our countries in IRENA data")
                query_items.append({"code": code, "selection": {"filter": "item", "values": our_countries}})
            elif 'indicator' in text:
                query_items.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
            elif 'year' in text:
                query_items.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
            else:
                query_items.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
        
        query = {"query": query_items, "response": {"format": "json-stat2"}}
        data = query_pxweb_table(table_path, query)
        
        if data:
            records = parse_jsonstat2(data, "IRENA_RE_SHARE", "Renewable energy share", "%", "Environment")
            
            if records:
                for r in records:
                    cur.execute("""
                        INSERT INTO unified_indicators 
                        (source, country_iso3, country_name, indicator_code, indicator_name, 
                         category, year, value, units)
                        VALUES ('IRENA', %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, country_iso3, indicator_code, year) 
                        DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                    """, (r['country_iso3'], r['country_name'], r['indicator_code'],
                          r['indicator_name'], r['category'], r['year'], r['value'], r['units']))
                
                conn.commit()
                total_inserted += len(records)
                print(f"    -> {len(records)} records added")
            else:
                print("    -> No records parsed")
        else:
            print("    -> Query failed")
    else:
        print("    -> Could not get metadata")
    
    time.sleep(2)
    
    # === 2. Electricity Statistics (Capacity & Generation) ===
    print("\n[2/2] Fetching Electricity Capacity & Generation data...")
    table_path2 = "Power%20Capacity%20and%20Generation/Country_ELECSTAT_2025_H2_PX.px"
    
    meta2 = get_table_metadata(table_path2)
    if meta2:
        variables = meta2.get('variables', [])
        query_items = []
        
        for var in variables:
            code = var.get('code', '')
            vals = var.get('values', [])
            text = var.get('text', '').lower()
            
            if 'country' in text or 'area' in text:
                our_countries = [v for v in vals if v in COUNTRIES]
                query_items.append({"code": code, "selection": {"filter": "item", "values": our_countries}})
            elif 'technology' in text:
                # Select total renewable
                re_tech = [v for v in vals if 'total renewable' in v.lower() or v == 'Total renewable energy']
                if re_tech:
                    query_items.append({"code": code, "selection": {"filter": "item", "values": re_tech}})
                else:
                    query_items.append({"code": code, "selection": {"filter": "top", "values": ["1"]}})
            elif 'data type' in text:
                # Get both capacity and generation
                query_items.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
            elif 'year' in text:
                query_items.append({"code": code, "selection": {"filter": "all", "values": ["*"]}})
            else:
                query_items.append({"code": code, "selection": {"filter": "top", "values": ["1"]}})
        
        query2 = {"query": query_items, "response": {"format": "json-stat2"}}
        data2 = query_pxweb_table(table_path2, query2)
        
        if data2:
            # Parse capacity data
            records2 = []
            
            dimensions = data2.get('dimension', {})
            values = data2.get('value', [])
            dim_ids = data2.get('id', [])
            dim_sizes = data2.get('size', [])
            
            # Build index maps
            idx_maps = {}
            for i, dim_id in enumerate(dim_ids):
                dim = dimensions.get(dim_id, {})
                cat = dim.get('category', {})
                idx_map = cat.get('index', {})
                label_map = cat.get('label', {})
                idx_maps[dim_id] = {
                    'idx': i,
                    'reverse': {v: k for k, v in idx_map.items()},
                    'labels': label_map
                }
            
            # Find dimension indices
            country_dim = next((k for k in dim_ids if 'country' in dimensions.get(k,{}).get('label','').lower() or 'area' in dimensions.get(k,{}).get('label','').lower()), None)
            year_dim = next((k for k in dim_ids if 'year' in dimensions.get(k,{}).get('label','').lower()), None)
            datatype_dim = next((k for k in dim_ids if 'data' in dimensions.get(k,{}).get('label','').lower()), None)
            
            if country_dim and year_dim:
                for flat_idx, value in enumerate(values):
                    if value is None:
                        continue
                    
                    indices = []
                    remaining = flat_idx
                    for size in reversed(dim_sizes):
                        indices.insert(0, remaining % size)
                        remaining //= size
                    
                    # Get country
                    c_idx = idx_maps[country_dim]['idx']
                    country_code = idx_maps[country_dim]['reverse'].get(indices[c_idx], '')
                    if country_code not in COUNTRIES:
                        continue
                    
                    # Get year
                    y_idx = idx_maps[year_dim]['idx']
                    year_str = idx_maps[year_dim]['reverse'].get(indices[y_idx], '')
                    try:
                        year = int(year_str)
                    except:
                        continue
                    
                    # Get data type (capacity or generation)
                    ind_code = "IRENA_RE_CAPACITY"
                    ind_name = "Total renewable electricity capacity"
                    units = "MW"
                    
                    if datatype_dim:
                        dt_idx = idx_maps[datatype_dim]['idx']
                        dt_code = idx_maps[datatype_dim]['reverse'].get(indices[dt_idx], '')
                        dt_label = idx_maps[datatype_dim]['labels'].get(dt_code, '')
                        if 'generation' in dt_label.lower() or 'gwh' in dt_label.lower():
                            ind_code = "IRENA_RE_GENERATION"
                            ind_name = "Total renewable electricity generation"
                            units = "GWh"
                    
                    records2.append({
                        'country_iso3': country_code,
                        'country_name': COUNTRY_NAMES.get(country_code, country_code),
                        'indicator_code': ind_code,
                        'indicator_name': ind_name,
                        'units': units,
                        'category': 'Environment',
                        'year': year,
                        'value': float(value)
                    })
                
                if records2:
                    for r in records2:
                        cur.execute("""
                            INSERT INTO unified_indicators 
                            (source, country_iso3, country_name, indicator_code, indicator_name, 
                             category, year, value, units)
                            VALUES ('IRENA', %s, %s, %s, %s, %s, %s, %s, %s)
                            ON CONFLICT (source, country_iso3, indicator_code, year) 
                            DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                        """, (r['country_iso3'], r['country_name'], r['indicator_code'],
                              r['indicator_name'], r['category'], r['year'], r['value'], r['units']))
                    
                    conn.commit()
                    total_inserted += len(records2)
                    print(f"    -> {len(records2)} records added")
                else:
                    print("    -> No records parsed")
            else:
                print("    -> Could not find dimensions")
        else:
            print("    -> Query failed")
    else:
        print("    -> Could not get metadata")
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'IRENA'")
    irena_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'IRENA'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM unified_indicators WHERE source = 'IRENA'")
    countries = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators WHERE source = 'IRENA'")
    years = cur.fetchone()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    print(f"Total IRENA records: {irena_total}")
    print(f"IRENA indicators: {indicators}")
    print(f"IRENA countries: {countries}")
    if years[0]:
        print(f"IRENA year range: {years[0]} - {years[1]}")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
