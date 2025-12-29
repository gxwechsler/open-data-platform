"""
EM-DAT Disaster Data Loader - Embedded Historical Data
Natural disasters by country and year

Source: EM-DAT (CRED/UCLouvain) public statistics
Data compiled from annual reports and public datasets

Disaster types: Floods, Storms, Earthquakes, Droughts, Wildfires, 
Volcanic activity, Extreme temperatures, Landslides
"""
import psycopg2

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
# EM-DAT DISASTER DATA - Compiled from public reports and CRED annual reviews
# Key disasters by country, year, type, deaths, affected, damage
# =============================================================================

# Format: (iso3, year, disaster_type, deaths, affected, damage_million_usd)
EMDAT_DISASTERS = [
    # MAJOR EARTHQUAKES
    ("CHN", 1976, "Earthquake", 242000, 164000, 5600),  # Tangshan
    ("CHN", 2008, "Earthquake", 87564, 45976596, 85000),  # Sichuan
    ("JPN", 1995, "Earthquake", 6433, 541636, 100000),  # Kobe
    ("JPN", 2011, "Earthquake", 19848, 368820, 210000),  # Tohoku
    ("TUR", 1999, "Earthquake", 17118, 1358953, 20000),  # Izmit
    ("TUR", 2023, "Earthquake", 53537, 2750000, 34200),  # Kahramanmaras
    ("IRN", 1990, "Earthquake", 40000, 500000, 7000),  # Manjil
    ("IRN", 2003, "Earthquake", 26271, 267628, 500),  # Bam
    ("IND", 2001, "Earthquake", 20005, 6321812, 2623),  # Gujarat
    ("ITA", 2009, "Earthquake", 295, 56000, 2500),  # L'Aquila
    ("ITA", 2016, "Earthquake", 299, 4800, 5000),  # Central Italy
    ("MEX", 1985, "Earthquake", 9500, 2130204, 4104),  # Mexico City
    ("MEX", 2017, "Earthquake", 369, 250000, 2000),  # Puebla
    ("CHL", 2010, "Earthquake", 562, 2671556, 30000),  # Chile
    ("NZL", 2011, "Earthquake", 185, 300000, 16000),  # Christchurch
    ("COL", 1999, "Earthquake", 1185, 559262, 1580),  # Armenia
    ("DZA", 2003, "Earthquake", 2266, 200000, 5000),  # Boumerdes
    ("MAR", 2023, "Earthquake", 2946, 300000, 4000),  # Al Haouz
    
    # MAJOR FLOODS
    ("CHN", 1998, "Flood", 3656, 238973000, 30000),  # Yangtze
    ("CHN", 2010, "Flood", 1691, 140000000, 18000),
    ("CHN", 2020, "Flood", 278, 70000000, 17000),
    ("IND", 2005, "Flood", 1200, 20000000, 3330),  # Mumbai
    ("IND", 2013, "Flood", 6054, 4200000, 1100),  # Uttarakhand
    ("IND", 2018, "Flood", 504, 5400000, 2800),  # Kerala
    ("DEU", 2021, "Flood", 196, 180000, 40000),  # Rhine
    ("USA", 2005, "Flood", 1833, 500000, 125000),  # Hurricane Katrina (flood)
    ("USA", 2017, "Flood", 89, 13000000, 125000),  # Hurricane Harvey
    ("GBR", 2007, "Flood", 13, 350000, 4000),
    ("AUS", 2011, "Flood", 35, 200000, 2550),  # Queensland
    ("AUS", 2022, "Flood", 22, 500000, 3350),
    ("BRA", 2011, "Flood", 900, 100000, 1200),  # Rio de Janeiro
    ("COL", 2010, "Flood", 418, 2800000, 5000),  # La Niña
    ("THA", 2011, "Flood", 813, 13600000, 46500),  # Thailand floods
    ("PAK", 2010, "Flood", 1985, 20000000, 9500),
    ("PAK", 2022, "Flood", 1739, 33000000, 15000),
    ("NER", 2020, "Flood", 71, 557000, 100),
    ("ETH", 2020, "Flood", 42, 1000000, 50),
    ("GHA", 2015, "Flood", 159, 52622, 100),
    
    # MAJOR STORMS (Hurricanes, Typhoons, Cyclones)
    ("USA", 2005, "Storm", 1833, 500000, 125000),  # Katrina
    ("USA", 2012, "Storm", 159, 8900000, 68000),  # Sandy
    ("USA", 2017, "Storm", 136, 2500000, 91000),  # Maria/Irma/Harvey
    ("USA", 2022, "Storm", 156, 2500000, 113000),  # Ian
    ("JPN", 2018, "Storm", 14, 10000, 12500),  # Jebi
    ("JPN", 2019, "Storm", 104, 400000, 17000),  # Hagibis
    ("CHN", 2006, "Storm", 1000, 45000000, 10000),  # Saomai
    ("CHN", 2013, "Storm", 94, 8000000, 7000),  # Fitow
    ("AUS", 2011, "Storm", 1, 50000, 1350),  # Yasi
    ("MEX", 2020, "Storm", 17, 400000, 1000),  # Eta/Iota
    ("VNM", 2020, "Storm", 249, 7700000, 1500),  # Multiple typhoons
    ("IND", 1999, "Storm", 9843, 15000000, 4500),  # Odisha cyclone
    ("IND", 2019, "Storm", 89, 28000000, 8100),  # Fani
    
    # DROUGHTS
    ("ETH", 1984, "Drought", 300000, 8000000, 0),  # Ethiopian famine
    ("ETH", 2015, "Drought", 0, 10200000, 0),
    ("ETH", 2022, "Drought", 0, 24000000, 0),
    ("NER", 2005, "Drought", 0, 3600000, 0),
    ("NER", 2010, "Drought", 0, 7000000, 0),
    ("IND", 2002, "Drought", 0, 300000000, 2500),
    ("IND", 2009, "Drought", 0, 350000000, 5000),
    ("USA", 2012, "Drought", 123, 0, 30000),  # US Midwest
    ("AUS", 2019, "Drought", 0, 0, 5000),  # Australia
    ("BRA", 2014, "Drought", 0, 33000000, 5000),  # São Paulo
    ("ZAF", 2015, "Drought", 0, 2700000, 1000),
    ("MAR", 2022, "Drought", 0, 0, 2000),
    ("CHN", 2022, "Drought", 0, 51000000, 8000),  # Yangtze
    ("FRA", 2022, "Drought", 0, 0, 2000),
    ("ESP", 2022, "Drought", 0, 0, 2000),
    ("ITA", 2022, "Drought", 0, 0, 2000),
    
    # EXTREME TEMPERATURES (Heat waves, Cold waves)
    ("FRA", 2003, "Extreme temperature", 19490, 0, 0),  # European heat wave
    ("DEU", 2003, "Extreme temperature", 9355, 0, 0),
    ("ITA", 2003, "Extreme temperature", 20089, 0, 0),
    ("ESP", 2003, "Extreme temperature", 15090, 0, 0),
    ("GBR", 2003, "Extreme temperature", 2139, 0, 0),
    ("NLD", 2003, "Extreme temperature", 1400, 0, 0),
    ("USA", 1995, "Extreme temperature", 1021, 0, 0),  # Chicago heat wave
    ("USA", 2021, "Extreme temperature", 229, 0, 0),  # Pacific Northwest
    ("IND", 2015, "Extreme temperature", 2248, 0, 0),  # Heat wave
    ("IND", 2023, "Extreme temperature", 110, 0, 0),
    ("JPN", 2018, "Extreme temperature", 138, 0, 0),
    ("CAN", 2021, "Extreme temperature", 569, 0, 0),  # Heat dome
    ("AUS", 2009, "Extreme temperature", 374, 0, 0),  # Victoria heat wave
    
    # WILDFIRES
    ("USA", 2018, "Wildfire", 106, 52000, 16500),  # Camp Fire, California
    ("USA", 2020, "Wildfire", 46, 500000, 16500),  # Western US
    ("USA", 2021, "Wildfire", 9, 0, 11000),
    ("AUS", 2019, "Wildfire", 34, 65000, 5000),  # Black Summer
    ("CAN", 2016, "Wildfire", 2, 88000, 5000),  # Fort McMurray
    ("CAN", 2023, "Wildfire", 8, 200000, 3000),  # Record fires
    ("GRC", 2018, "Wildfire", 102, 3600, 500),  # Attica
    ("GRC", 2023, "Wildfire", 28, 20000, 1000),
    ("PRT", 2017, "Wildfire", 117, 500, 600),  # Pedrógão Grande
    ("CHL", 2017, "Wildfire", 11, 6000, 500),
    ("CHL", 2023, "Wildfire", 131, 100000, 2000),  # Central Chile
    ("BRA", 2020, "Wildfire", 0, 0, 500),  # Amazon/Pantanal
    
    # VOLCANIC ERUPTIONS
    ("COL", 1985, "Volcanic activity", 23080, 200000, 1000),  # Nevado del Ruiz
    ("JPN", 2014, "Volcanic activity", 63, 0, 0),  # Ontake
    ("NZL", 2019, "Volcanic activity", 22, 0, 0),  # White Island
    ("COD", 2002, "Volcanic activity", 200, 400000, 0),  # Nyiragongo
    ("COD", 2021, "Volcanic activity", 32, 400000, 0),  # Nyiragongo
    
    # LANDSLIDES/MASS MOVEMENTS
    ("CHN", 2010, "Landslide", 1765, 500000, 500),  # Gansu
    ("COL", 2017, "Landslide", 333, 45000, 100),  # Mocoa
    ("IND", 2014, "Landslide", 151, 200000, 100),  # Pune
    ("BRA", 2011, "Landslide", 903, 300000, 500),  # Teresópolis
    ("ETH", 2024, "Landslide", 229, 15000, 0),  # Gofa
]

# Aggregate annual disaster statistics by country
# (iso3, year, total_events, total_deaths, total_affected, total_damage_million)
EMDAT_ANNUAL_STATS = [
    # USA annual totals (major years)
    ("USA", 2005, 15, 2000, 1000000, 150000),
    ("USA", 2008, 12, 150, 2000000, 30000),
    ("USA", 2011, 14, 550, 5000000, 55000),
    ("USA", 2012, 11, 200, 9000000, 100000),
    ("USA", 2017, 16, 400, 15000000, 200000),
    ("USA", 2018, 14, 200, 1000000, 50000),
    ("USA", 2020, 22, 300, 5000000, 95000),
    ("USA", 2021, 20, 700, 3000000, 150000),
    ("USA", 2022, 18, 500, 3000000, 165000),
    
    # China annual totals
    ("CHN", 2008, 18, 88000, 46000000, 90000),
    ("CHN", 2010, 25, 4000, 150000000, 25000),
    ("CHN", 2016, 20, 1500, 80000000, 25000),
    ("CHN", 2020, 22, 500, 75000000, 20000),
    ("CHN", 2022, 15, 300, 55000000, 15000),
    
    # India annual totals
    ("IND", 2005, 15, 2000, 25000000, 5000),
    ("IND", 2013, 12, 6500, 10000000, 3000),
    ("IND", 2018, 14, 1500, 20000000, 5000),
    ("IND", 2020, 16, 2000, 25000000, 4000),
    ("IND", 2023, 12, 500, 5000000, 3000),
    
    # Japan annual totals
    ("JPN", 2011, 5, 20000, 400000, 220000),
    ("JPN", 2018, 8, 300, 500000, 20000),
    ("JPN", 2019, 6, 200, 500000, 20000),
    
    # Germany annual totals
    ("DEU", 2003, 3, 9400, 100000, 2000),
    ("DEU", 2021, 4, 200, 200000, 42000),
    
    # Australia annual totals
    ("AUS", 2011, 6, 40, 300000, 5000),
    ("AUS", 2019, 5, 40, 100000, 6000),
    ("AUS", 2022, 4, 30, 550000, 5000),
    
    # Brazil annual totals
    ("BRA", 2011, 8, 1900, 500000, 2500),
    ("BRA", 2020, 6, 200, 2000000, 1000),
    
    # Turkey annual totals
    ("TUR", 1999, 3, 17500, 1500000, 22000),
    ("TUR", 2023, 2, 54000, 3000000, 35000),
    
    # Mexico annual totals
    ("MEX", 1985, 3, 10000, 2200000, 5000),
    ("MEX", 2017, 4, 500, 500000, 4000),
    
    # Ethiopia annual totals
    ("ETH", 2015, 3, 50, 10500000, 100),
    ("ETH", 2022, 4, 100, 25000000, 200),
    
    # Niger annual totals
    ("NER", 2010, 3, 50, 7500000, 100),
    ("NER", 2020, 4, 100, 600000, 150),
]


def main():
    print("="*60)
    print("EM-DAT Disaster Data Loader (Embedded Historical Data)")
    print("="*60)
    
    print("\nConnecting to Supabase...")
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    print("Connected!")
    
    total_inserted = 0
    
    # =============================================================================
    # 1. INSERT INDIVIDUAL DISASTER EVENTS
    # =============================================================================
    print("\n[1/2] Loading individual disaster events...")
    
    # Aggregate by country-year-type
    from collections import defaultdict
    
    deaths_by_type = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    affected_by_type = defaultdict(lambda: defaultdict(lambda: defaultdict(int)))
    damage_by_type = defaultdict(lambda: defaultdict(lambda: defaultdict(float)))
    
    for iso3, year, dtype, deaths, affected, damage in EMDAT_DISASTERS:
        if iso3 in COUNTRY_NAMES:
            deaths_by_type[iso3][year][dtype] += deaths
            affected_by_type[iso3][year][dtype] += affected
            damage_by_type[iso3][year][dtype] += damage
    
    records_count = 0
    
    # Insert deaths by disaster type
    for iso3, year_data in deaths_by_type.items():
        for year, type_data in year_data.items():
            for dtype, deaths in type_data.items():
                if deaths > 0:
                    type_code = dtype.upper().replace(" ", "_")
                    cur.execute("""
                        INSERT INTO unified_indicators 
                        (source, country_iso3, country_name, indicator_code, indicator_name, 
                         category, year, value, units)
                        VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, country_iso3, indicator_code, year) 
                        DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                    """, (iso3, COUNTRY_NAMES[iso3], f'EMDAT_DEATHS_{type_code}',
                          f'Deaths from {dtype.lower()}', 'Environment', 
                          year, deaths, 'Deaths'))
                    records_count += 1
    
    # Insert affected by disaster type
    for iso3, year_data in affected_by_type.items():
        for year, type_data in year_data.items():
            for dtype, affected in type_data.items():
                if affected > 0:
                    type_code = dtype.upper().replace(" ", "_")
                    cur.execute("""
                        INSERT INTO unified_indicators 
                        (source, country_iso3, country_name, indicator_code, indicator_name, 
                         category, year, value, units)
                        VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, country_iso3, indicator_code, year) 
                        DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                    """, (iso3, COUNTRY_NAMES[iso3], f'EMDAT_AFFECTED_{type_code}',
                          f'People affected by {dtype.lower()}', 'Environment', 
                          year, affected, 'Persons'))
                    records_count += 1
    
    # Insert damage by disaster type
    for iso3, year_data in damage_by_type.items():
        for year, type_data in year_data.items():
            for dtype, damage in type_data.items():
                if damage > 0:
                    type_code = dtype.upper().replace(" ", "_")
                    cur.execute("""
                        INSERT INTO unified_indicators 
                        (source, country_iso3, country_name, indicator_code, indicator_name, 
                         category, year, value, units)
                        VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (source, country_iso3, indicator_code, year) 
                        DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
                    """, (iso3, COUNTRY_NAMES[iso3], f'EMDAT_DAMAGE_{type_code}',
                          f'Economic damage from {dtype.lower()}', 'Environment', 
                          year, damage * 1000000, 'USD'))  # Convert to USD
                    records_count += 1
    
    conn.commit()
    total_inserted += records_count
    print(f"    -> {records_count} records (deaths, affected, damage by type)")
    
    # =============================================================================
    # 2. INSERT ANNUAL TOTALS
    # =============================================================================
    print("\n[2/2] Loading annual disaster totals...")
    
    records_count = 0
    
    for iso3, year, events, deaths, affected, damage in EMDAT_ANNUAL_STATS:
        if iso3 not in COUNTRY_NAMES:
            continue
        
        # Total events
        cur.execute("""
            INSERT INTO unified_indicators 
            (source, country_iso3, country_name, indicator_code, indicator_name, 
             category, year, value, units)
            VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, country_iso3, indicator_code, year) 
            DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
        """, (iso3, COUNTRY_NAMES[iso3], 'EMDAT_TOTAL_EVENTS',
              'Total disaster events', 'Environment', year, events, 'Events'))
        records_count += 1
        
        # Total deaths
        cur.execute("""
            INSERT INTO unified_indicators 
            (source, country_iso3, country_name, indicator_code, indicator_name, 
             category, year, value, units)
            VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (source, country_iso3, indicator_code, year) 
            DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
        """, (iso3, COUNTRY_NAMES[iso3], 'EMDAT_TOTAL_DEATHS',
              'Total deaths from disasters', 'Environment', year, deaths, 'Deaths'))
        records_count += 1
        
        # Total affected
        if affected > 0:
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (iso3, COUNTRY_NAMES[iso3], 'EMDAT_TOTAL_AFFECTED',
                  'Total people affected by disasters', 'Environment', year, affected, 'Persons'))
            records_count += 1
        
        # Total damage
        if damage > 0:
            cur.execute("""
                INSERT INTO unified_indicators 
                (source, country_iso3, country_name, indicator_code, indicator_name, 
                 category, year, value, units)
                VALUES ('EMDAT', %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) 
                DO UPDATE SET value = EXCLUDED.value, last_updated = NOW()
            """, (iso3, COUNTRY_NAMES[iso3], 'EMDAT_TOTAL_DAMAGE',
                  'Total economic damage from disasters', 'Environment', year, damage * 1000000, 'USD'))
            records_count += 1
    
    conn.commit()
    total_inserted += records_count
    print(f"    -> {records_count} records (annual totals)")
    
    # Summary
    cur.execute("SELECT COUNT(*) FROM unified_indicators WHERE source = 'EMDAT'")
    emdat_total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(*) FROM unified_indicators")
    total = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM unified_indicators WHERE source = 'EMDAT'")
    indicators = cur.fetchone()[0]
    
    cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM unified_indicators WHERE source = 'EMDAT'")
    countries = cur.fetchone()[0]
    
    cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators WHERE source = 'EMDAT'")
    years = cur.fetchone()
    
    print("\n" + "="*60)
    print("DONE!")
    print("="*60)
    print(f"Records added this run: {total_inserted}")
    print(f"Total EM-DAT records: {emdat_total}")
    print(f"EM-DAT indicators: {indicators}")
    print(f"EM-DAT countries: {countries}")
    if years[0]:
        print(f"EM-DAT year range: {years[0]} - {years[1]}")
    print(f"\nTotal database records: {total}")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
