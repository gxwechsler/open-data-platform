"""
Crisis Data Loader - Loads Laeven-Valencia and Reinhart-Rogoff crisis data
into unified_indicators table.

Sources:
- Laeven-Valencia: IMF Systemic Banking Crises Database (1970-2017)
- Reinhart-Rogoff: This Time Is Different (1800-2010)
"""
import psycopg2
from collections import defaultdict

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

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

# =============================================================================
# LAEVEN-VALENCIA BANKING CRISES DATA
# Source: IMF WP/20/206 "Systemic Banking Crises Database II"
# =============================================================================

LAEVEN_VALENCIA_BANKING = [
    # Argentina
    {"iso3": "ARG", "year": 1980, "output_loss": 58.2, "fiscal_cost": 55.1, "peak_npl": 9.0},
    {"iso3": "ARG", "year": 1989, "output_loss": 27.0, "fiscal_cost": 6.0, "peak_npl": 27.0},
    {"iso3": "ARG", "year": 1995, "output_loss": None, "fiscal_cost": 2.0, "peak_npl": 12.3},
    {"iso3": "ARG", "year": 2001, "output_loss": 42.7, "fiscal_cost": 9.6, "peak_npl": 20.1},
    # Brazil
    {"iso3": "BRA", "year": 1990, "output_loss": 0.0, "fiscal_cost": 0.0, "peak_npl": None},
    {"iso3": "BRA", "year": 1994, "output_loss": 0.0, "fiscal_cost": 13.2, "peak_npl": 16.0},
    # Chile
    {"iso3": "CHL", "year": 1981, "output_loss": 92.4, "fiscal_cost": 42.9, "peak_npl": 35.6},
    # Colombia
    {"iso3": "COL", "year": 1982, "output_loss": 26.4, "fiscal_cost": 5.0, "peak_npl": 25.0},
    {"iso3": "COL", "year": 1998, "output_loss": 43.4, "fiscal_cost": 6.3, "peak_npl": 14.0},
    # Mexico
    {"iso3": "MEX", "year": 1981, "output_loss": 26.6, "fiscal_cost": 0.0, "peak_npl": None},
    {"iso3": "MEX", "year": 1994, "output_loss": 9.7, "fiscal_cost": 19.3, "peak_npl": 18.9},
    # USA
    {"iso3": "USA", "year": 1988, "output_loss": 0.0, "fiscal_cost": 3.7, "peak_npl": 4.1},
    {"iso3": "USA", "year": 2007, "output_loss": 31.0, "fiscal_cost": 4.5, "peak_npl": 5.0},
    # UK
    {"iso3": "GBR", "year": 2007, "output_loss": 23.8, "fiscal_cost": 8.8, "peak_npl": 4.0},
    # Germany
    {"iso3": "DEU", "year": 2008, "output_loss": 5.8, "fiscal_cost": 1.8, "peak_npl": 3.3},
    # France
    {"iso3": "FRA", "year": 2008, "output_loss": 23.0, "fiscal_cost": 1.0, "peak_npl": 4.0},
    # Spain
    {"iso3": "ESP", "year": 2008, "output_loss": 38.2, "fiscal_cost": 5.4, "peak_npl": 9.4},
    # Italy
    {"iso3": "ITA", "year": 2008, "output_loss": 31.9, "fiscal_cost": 0.3, "peak_npl": 9.5},
    # Netherlands
    {"iso3": "NLD", "year": 2008, "output_loss": 22.5, "fiscal_cost": 12.7, "peak_npl": 3.2},
    # Switzerland
    {"iso3": "CHE", "year": 2008, "output_loss": 0.0, "fiscal_cost": 1.1, "peak_npl": 0.8},
    # Japan
    {"iso3": "JPN", "year": 1997, "output_loss": 45.0, "fiscal_cost": 14.0, "peak_npl": 35.0},
    # China
    {"iso3": "CHN", "year": 1998, "output_loss": 0.0, "fiscal_cost": 18.0, "peak_npl": 20.0},
    # India
    {"iso3": "IND", "year": 1993, "output_loss": 0.0, "fiscal_cost": 0.0, "peak_npl": 15.7},
    # Turkey
    {"iso3": "TUR", "year": 2000, "output_loss": 18.3, "fiscal_cost": 32.0, "peak_npl": 27.6},
    # Finland
    {"iso3": "FIN", "year": 1991, "output_loss": 59.1, "fiscal_cost": 12.8, "peak_npl": 13.0},
    # Sweden
    {"iso3": "SWE", "year": 1991, "output_loss": 33.0, "fiscal_cost": 3.6, "peak_npl": 13.0},
    # Norway
    {"iso3": "NOR", "year": 1991, "output_loss": 5.1, "fiscal_cost": 2.7, "peak_npl": 16.4},
    # Denmark
    {"iso3": "DNK", "year": 2008, "output_loss": 27.3, "fiscal_cost": 5.9, "peak_npl": 4.1},
    # Ireland
    {"iso3": "IRL", "year": 2008, "output_loss": 106.0, "fiscal_cost": 40.7, "peak_npl": 25.0},
    # Australia
    {"iso3": "AUS", "year": 1989, "output_loss": 0.0, "fiscal_cost": 1.9, "peak_npl": 6.0},
]

# =============================================================================
# REINHART-ROGOFF HISTORICAL CRISES
# Source: "This Time Is Different" (2009)
# =============================================================================

# Count of crises by country and decade for visualization
REINHART_ROGOFF_CRISES = [
    # Sovereign Defaults
    {"iso3": "GBR", "year": 1340, "type": "SOVEREIGN", "notes": "Edward III default"},
    {"iso3": "ESP", "year": 1557, "type": "SOVEREIGN", "notes": "Philip II - first of 7 defaults"},
    {"iso3": "ESP", "year": 1575, "type": "SOVEREIGN"},
    {"iso3": "ESP", "year": 1596, "type": "SOVEREIGN"},
    {"iso3": "ESP", "year": 1607, "type": "SOVEREIGN"},
    {"iso3": "ESP", "year": 1627, "type": "SOVEREIGN"},
    {"iso3": "ESP", "year": 1647, "type": "SOVEREIGN"},
    {"iso3": "FRA", "year": 1788, "type": "SOVEREIGN", "notes": "Precipitated French Revolution"},
    {"iso3": "ARG", "year": 1827, "type": "SOVEREIGN"},
    {"iso3": "ARG", "year": 1890, "type": "SOVEREIGN", "notes": "Baring Crisis"},
    {"iso3": "ARG", "year": 1951, "type": "SOVEREIGN"},
    {"iso3": "ARG", "year": 1956, "type": "SOVEREIGN"},
    {"iso3": "ARG", "year": 1982, "type": "SOVEREIGN"},
    {"iso3": "ARG", "year": 1989, "type": "SOVEREIGN"},
    {"iso3": "ARG", "year": 2001, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1828, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1898, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1902, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1914, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1931, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1937, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1961, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1964, "type": "SOVEREIGN"},
    {"iso3": "BRA", "year": 1983, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1827, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1833, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1844, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1866, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1914, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1928, "type": "SOVEREIGN"},
    {"iso3": "MEX", "year": 1982, "type": "SOVEREIGN"},
    {"iso3": "CHL", "year": 1826, "type": "SOVEREIGN"},
    {"iso3": "CHL", "year": 1880, "type": "SOVEREIGN"},
    {"iso3": "CHL", "year": 1931, "type": "SOVEREIGN"},
    {"iso3": "CHL", "year": 1961, "type": "SOVEREIGN"},
    {"iso3": "CHL", "year": 1972, "type": "SOVEREIGN"},
    {"iso3": "CHL", "year": 1983, "type": "SOVEREIGN"},
    {"iso3": "COL", "year": 1826, "type": "SOVEREIGN"},
    {"iso3": "COL", "year": 1879, "type": "SOVEREIGN"},
    {"iso3": "COL", "year": 1900, "type": "SOVEREIGN"},
    {"iso3": "COL", "year": 1932, "type": "SOVEREIGN"},
    {"iso3": "TUR", "year": 1876, "type": "SOVEREIGN"},
    {"iso3": "TUR", "year": 1915, "type": "SOVEREIGN"},
    {"iso3": "TUR", "year": 1931, "type": "SOVEREIGN"},
    {"iso3": "TUR", "year": 1940, "type": "SOVEREIGN"},
    {"iso3": "TUR", "year": 1978, "type": "SOVEREIGN"},
    {"iso3": "TUR", "year": 1982, "type": "SOVEREIGN"},
    {"iso3": "EGY", "year": 1876, "type": "SOVEREIGN"},
    {"iso3": "EGY", "year": 1984, "type": "SOVEREIGN"},
    {"iso3": "ZAF", "year": 1985, "type": "SOVEREIGN"},
    {"iso3": "ZAF", "year": 1989, "type": "SOVEREIGN"},
    {"iso3": "MAR", "year": 1903, "type": "SOVEREIGN"},
    {"iso3": "MAR", "year": 1983, "type": "SOVEREIGN"},
    {"iso3": "TUN", "year": 1867, "type": "SOVEREIGN"},
    {"iso3": "DZA", "year": 1991, "type": "SOVEREIGN"},
    {"iso3": "NER", "year": 1983, "type": "SOVEREIGN"},
    {"iso3": "GHA", "year": 1966, "type": "SOVEREIGN"},
    {"iso3": "GHA", "year": 1968, "type": "SOVEREIGN"},
    {"iso3": "GHA", "year": 1970, "type": "SOVEREIGN"},
    {"iso3": "GHA", "year": 1974, "type": "SOVEREIGN"},
    {"iso3": "GHA", "year": 1987, "type": "SOVEREIGN"},
    {"iso3": "COD", "year": 1976, "type": "SOVEREIGN"},
    {"iso3": "COD", "year": 1977, "type": "SOVEREIGN"},
    {"iso3": "ETH", "year": 1991, "type": "SOVEREIGN"},
    {"iso3": "CHN", "year": 1921, "type": "SOVEREIGN"},
    {"iso3": "CHN", "year": 1939, "type": "SOVEREIGN"},
    {"iso3": "IND", "year": 1958, "type": "SOVEREIGN"},
    {"iso3": "IND", "year": 1969, "type": "SOVEREIGN"},
    {"iso3": "IND", "year": 1972, "type": "SOVEREIGN"},
    {"iso3": "DEU", "year": 1932, "type": "SOVEREIGN"},
    {"iso3": "DEU", "year": 1939, "type": "SOVEREIGN"},
    
    # Banking Crises
    {"iso3": "GBR", "year": 1825, "type": "BANKING", "notes": "Latin American bubble burst"},
    {"iso3": "GBR", "year": 1866, "type": "BANKING", "notes": "Overend Gurney collapse"},
    {"iso3": "GBR", "year": 1890, "type": "BANKING", "notes": "Baring Crisis"},
    {"iso3": "GBR", "year": 1974, "type": "BANKING"},
    {"iso3": "GBR", "year": 1991, "type": "BANKING"},
    {"iso3": "GBR", "year": 1995, "type": "BANKING", "notes": "Barings Bank collapse"},
    {"iso3": "USA", "year": 1857, "type": "BANKING", "notes": "Panic of 1857"},
    {"iso3": "USA", "year": 1873, "type": "BANKING", "notes": "Long Depression"},
    {"iso3": "USA", "year": 1893, "type": "BANKING", "notes": "Panic of 1893"},
    {"iso3": "USA", "year": 1907, "type": "BANKING", "notes": "Panic of 1907"},
    {"iso3": "USA", "year": 1929, "type": "BANKING", "notes": "Great Depression"},
    {"iso3": "USA", "year": 1984, "type": "BANKING", "notes": "S&L Crisis"},
    {"iso3": "DEU", "year": 1931, "type": "BANKING", "notes": "Danat Bank failure"},
    {"iso3": "FRA", "year": 1930, "type": "BANKING"},
    {"iso3": "ITA", "year": 1930, "type": "BANKING"},
    {"iso3": "NLD", "year": 1921, "type": "BANKING"},
    {"iso3": "CHE", "year": 1931, "type": "BANKING"},
    {"iso3": "JPN", "year": 1927, "type": "BANKING"},
    {"iso3": "JPN", "year": 1992, "type": "BANKING", "notes": "Lost Decade begins"},
    {"iso3": "AUS", "year": 1893, "type": "BANKING"},
    {"iso3": "NZL", "year": 1893, "type": "BANKING"},
    {"iso3": "CAN", "year": 1983, "type": "BANKING"},
    {"iso3": "ARG", "year": 1890, "type": "BANKING"},
    {"iso3": "ARG", "year": 1931, "type": "BANKING"},
    {"iso3": "BRA", "year": 1890, "type": "BANKING"},
    {"iso3": "BRA", "year": 1897, "type": "BANKING"},
    {"iso3": "BRA", "year": 1900, "type": "BANKING"},
    {"iso3": "BRA", "year": 1914, "type": "BANKING"},
    {"iso3": "BRA", "year": 1923, "type": "BANKING"},
    {"iso3": "MEX", "year": 1908, "type": "BANKING"},
    {"iso3": "MEX", "year": 1929, "type": "BANKING"},
    {"iso3": "IND", "year": 1908, "type": "BANKING"},
    {"iso3": "IND", "year": 1913, "type": "BANKING"},
    {"iso3": "IND", "year": 1921, "type": "BANKING"},
    {"iso3": "CHN", "year": 1923, "type": "BANKING"},
    {"iso3": "CHN", "year": 1931, "type": "BANKING"},
    {"iso3": "CHN", "year": 1934, "type": "BANKING"},
    {"iso3": "TUR", "year": 1931, "type": "BANKING"},
    {"iso3": "TUR", "year": 1982, "type": "BANKING"},
    
    # Currency Crises
    {"iso3": "ARG", "year": 1876, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1890, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1914, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1929, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1949, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1958, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1967, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1975, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1981, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 1987, "type": "CURRENCY"},
    {"iso3": "ARG", "year": 2002, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1898, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1914, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1929, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1948, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1964, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1983, "type": "CURRENCY"},
    {"iso3": "BRA", "year": 1999, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1876, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1913, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1929, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1938, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1948, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1976, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1982, "type": "CURRENCY"},
    {"iso3": "MEX", "year": 1994, "type": "CURRENCY", "notes": "Tequila Crisis"},
    {"iso3": "TUR", "year": 1978, "type": "CURRENCY"},
    {"iso3": "TUR", "year": 1984, "type": "CURRENCY"},
    {"iso3": "TUR", "year": 1991, "type": "CURRENCY"},
    {"iso3": "TUR", "year": 1994, "type": "CURRENCY"},
    {"iso3": "TUR", "year": 1996, "type": "CURRENCY"},
    {"iso3": "TUR", "year": 2001, "type": "CURRENCY"},
]


def main():
    print("="*60)
    print("Crisis Data Loader - Laeven-Valencia & Reinhart-Rogoff")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    total_inserted = 0
    
    # =============================================================================
    # LAEVEN-VALENCIA DATA
    # =============================================================================
    print("\n[1/2] Loading Laeven-Valencia Banking Crisis data...")
    
    lv_records = 0
    
    # Output Loss (% of GDP)
    for crisis in LAEVEN_VALENCIA_BANKING:
        iso3 = crisis['iso3']
        if iso3 not in COUNTRY_NAMES:
            continue
        
        year = crisis['year']
        
        # Output Loss
        if crisis.get('output_loss') is not None:
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('LV', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (iso3, COUNTRY_NAMES[iso3], 'LV_OUTPUT_LOSS',
                  'Banking crisis output loss', 'Finance', year, 
                  crisis['output_loss'], '% of GDP'))
            lv_records += 1
        
        # Fiscal Cost
        if crisis.get('fiscal_cost') is not None:
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('LV', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (iso3, COUNTRY_NAMES[iso3], 'LV_FISCAL_COST',
                  'Banking crisis fiscal cost', 'Finance', year,
                  crisis['fiscal_cost'], '% of GDP'))
            lv_records += 1
        
        # Peak NPL
        if crisis.get('peak_npl') is not None:
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('LV', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (iso3, COUNTRY_NAMES[iso3], 'LV_PEAK_NPL',
                  'Peak non-performing loans', 'Finance', year,
                  crisis['peak_npl'], '%'))
            lv_records += 1
    
    conn.commit()
    total_inserted += lv_records
    print(f"    -> {lv_records} records added")
    
    # =============================================================================
    # REINHART-ROGOFF DATA
    # =============================================================================
    print("\n[2/2] Loading Reinhart-Rogoff Historical Crisis data...")
    
    # Aggregate crisis counts by country, year, and type
    crisis_counts = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    
    for crisis in REINHART_ROGOFF_CRISES:
        iso3 = crisis['iso3']
        if iso3 not in COUNTRY_NAMES:
            continue
        year = crisis['year']
        ctype = crisis['type']
        crisis_counts[iso3][year][ctype] += 1
    
    rr_records = 0
    
    for iso3, year_data in crisis_counts.items():
        for year, type_counts in year_data.items():
            for ctype, count in type_counts.items():
                if ctype == "SOVEREIGN":
                    indicator_code = "RR_SOVEREIGN_DEFAULT"
                    indicator_name = "Sovereign debt default/restructuring"
                elif ctype == "BANKING":
                    indicator_code = "RR_BANKING_CRISIS"
                    indicator_name = "Banking crisis"
                elif ctype == "CURRENCY":
                    indicator_code = "RR_CURRENCY_CRISIS"
                    indicator_name = "Currency crisis"
                else:
                    continue
                
                cur.execute("""
                    INSERT INTO unified_indicators 
                    (source, country_iso3, country_name, indicator_code, indicator_name, 
                     category, year, value, units)
                    VALUES ('RR', %s, %s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (source, country_iso3, indicator_code, year) 
                    DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                """, (iso3, COUNTRY_NAMES[iso3], indicator_code,
                      indicator_name, 'Finance', year, count, 'Events'))
                rr_records += 1
    
    conn.commit()
    total_inserted += rr_records
    print(f"    -> {rr_records} records added")
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source IN ('LV', 'RR')")
    crisis_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT source, COUNT(*) FROM unified_indicators WHERE source IN ('LV', 'RR') GROUP BY source")
    by_source = cur.fetchall()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    for src, cnt in by_source:
        src_name = "Laeven-Valencia" if src == "LV" else "Reinhart-Rogoff"
        print(f"  {src_name}: {cnt} records")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
