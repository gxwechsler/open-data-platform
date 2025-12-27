import psycopg2
import requests
import time

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

COUNTRIES = "ARG;BRA;CHL;COL;MEX;USA;CAN;DEU;FRA;ITA;SWE;NLD;CHE;DNK;FIN;NOR;TUR;ESP;GBR;IRL;IND;CHN;JPN;VNM;SGP;ISR;IRN;ARE;SAU;QAT;NER;ZAF;EGY;COD;MAR;DZA;ETH;LBY;TZA;TUN;GHA;AUS;NZL"

INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.PCAP.KD.ZG": "GDP per capita growth (annual %)",
    "NY.GNP.MKTP.CD": "GNI (current US$)",
    "NY.GNP.PCAP.CD": "GNI per capita (current US$)",
    "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
    "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
    "NE.TRD.GNFS.ZS": "Trade (% of GDP)",
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "SL.UEM.TOTL.FE.ZS": "Unemployment, female (% of labor force)",
    "SL.UEM.TOTL.MA.ZS": "Unemployment, male (% of labor force)",
    "SL.TLF.CACT.ZS": "Labor force participation rate, total (%)",
    "SL.TLF.CACT.FE.ZS": "Labor force participation rate, female (%)",
    "SL.EMP.TOTL.SP.ZS": "Employment to population ratio (%)",
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "FP.CPI.TOTL": "Consumer price index (2010 = 100)",
    "NY.GDP.DEFL.KD.ZG": "Inflation, GDP deflator (annual %)",
    "SP.POP.TOTL": "Population, total",
    "SP.POP.GROW": "Population growth (annual %)",
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "SP.POP.65UP.TO.ZS": "Population ages 65+ (% of total)",
    "SP.POP.0014.TO.ZS": "Population ages 0-14 (% of total)",
    "SP.DYN.CBRT.IN": "Birth rate, crude (per 1,000 people)",
    "SP.DYN.CDRT.IN": "Death rate, crude (per 1,000 people)",
    "SI.POV.DDAY": "Poverty headcount ratio at $2.15/day (%)",
    "SI.POV.LMIC": "Poverty headcount ratio at $3.65/day (%)",
    "SI.POV.GINI": "Gini index",
    "SI.DST.10TH.10": "Income share held by highest 10%",
    "SI.DST.FRST.10": "Income share held by lowest 10%",
    "SP.DYN.LE00.IN": "Life expectancy at birth (years)",
    "SP.DYN.LE00.FE.IN": "Life expectancy at birth, female (years)",
    "SP.DYN.LE00.MA.IN": "Life expectancy at birth, male (years)",
    "SH.DYN.MORT": "Mortality rate, under-5 (per 1,000 live births)",
    "SH.DYN.NMRT": "Mortality rate, neonatal (per 1,000 live births)",
    "SP.DYN.IMRT.IN": "Mortality rate, infant (per 1,000 live births)",
    "SH.XPD.CHEX.GD.ZS": "Current health expenditure (% of GDP)",
    "SH.MED.PHYS.ZS": "Physicians (per 1,000 people)",
    "SH.MED.BEDS.ZS": "Hospital beds (per 1,000 people)",
    "SE.ADT.LITR.ZS": "Literacy rate, adult total (%)",
    "SE.PRM.ENRR": "School enrollment, primary (% gross)",
    "SE.SEC.ENRR": "School enrollment, secondary (% gross)",
    "SE.TER.ENRR": "School enrollment, tertiary (% gross)",
    "SE.XPD.TOTL.GD.ZS": "Government expenditure on education (% of GDP)",
    "SE.PRM.CMPT.ZS": "Primary completion rate (%)",
    "IT.NET.USER.ZS": "Individuals using the Internet (%)",
    "IT.CEL.SETS.P2": "Mobile cellular subscriptions (per 100 people)",
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
    "EG.USE.ELEC.KH.PC": "Electric power consumption (kWh per capita)",
    "IS.AIR.PSGR": "Air transport, passengers carried",
    "EN.ATM.CO2E.PC": "CO2 emissions (metric tons per capita)",
    "EN.ATM.CO2E.KT": "CO2 emissions (kt)",
    "AG.LND.FRST.ZS": "Forest area (% of land area)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (%)",
    "EN.ATM.PM25.MC.M3": "PM2.5 air pollution (micrograms per cubic meter)",
    "FR.INR.RINR": "Real interest rate (%)",
    "FS.AST.DOMS.GD.ZS": "Domestic credit provided by financial sector (% of GDP)",
    "BX.KLT.DINV.WD.GD.ZS": "Foreign direct investment, net inflows (% of GDP)",
    "GC.DOD.TOTL.GD.ZS": "Central government debt, total (% of GDP)",
    "BN.CAB.XOKA.GD.ZS": "Current account balance (% of GDP)",
}

def fetch_indicator(indicator_code, countries):
    url = f"https://api.worldbank.org/v2/country/{countries}/indicator/{indicator_code}"
    params = {"format": "json", "date": "2000:2023", "per_page": 10000}
    try:
        response = requests.get(url, params=params, timeout=30)
        data = response.json()
        if len(data) < 2 or not data[1]:
            return []
        records = []
        for item in data[1]:
            if item['value'] is not None:
                records.append((
                    item['countryiso3code'],
                    item['country']['value'],
                    indicator_code,
                    item['indicator']['value'],
                    int(item['date']),
                    float(item['value'])
                ))
        return records
    except Exception as e:
        print(f"  Error: {e}")
        return []

print("Connecting to Supabase...")
conn = psycopg2.connect(DATABASE_URL)
cur = conn.cursor()

print("Creating worldbank_data table...")
cur.execute("DROP TABLE IF EXISTS worldbank_data")
cur.execute("""
    CREATE TABLE worldbank_data (
        id SERIAL PRIMARY KEY,
        country_iso3 VARCHAR(3),
        country VARCHAR(100),
        indicator_code VARCHAR(50),
        indicator_name VARCHAR(200),
        year INTEGER,
        value FLOAT,
        created_at TIMESTAMP DEFAULT NOW(),
        UNIQUE(country_iso3, indicator_code, year)
    )
""")
cur.execute("CREATE INDEX ix_wb_country ON worldbank_data(country_iso3)")
cur.execute("CREATE INDEX ix_wb_indicator ON worldbank_data(indicator_code)")
conn.commit()
print("Table created!")

print(f"\nFetching {len(INDICATORS)} indicators for 44 countries...")
print("This will take a few minutes...\n")

total_inserted = 0
for i, (code, name) in enumerate(INDICATORS.items(), 1):
    print(f"[{i}/{len(INDICATORS)}] {name}...")
    records = fetch_indicator(code, COUNTRIES)
    for r in records:
        try:
            cur.execute("""
                INSERT INTO worldbank_data (country_iso3, country, indicator_code, indicator_name, year, value)
                VALUES (%s, %s, %s, %s, %s, %s)
                ON CONFLICT (country_iso3, indicator_code, year) DO UPDATE SET value = EXCLUDED.value
            """, r)
        except:
            pass
    conn.commit()
    total_inserted += len(records)
    print(f"    -> {len(records)} records")
    time.sleep(0.5)

cur.execute("SELECT COUNT(*) FROM worldbank_data")
count = cur.fetchone()[0]

cur.execute("SELECT COUNT(DISTINCT country_iso3) FROM worldbank_data")
countries_count = cur.fetchone()[0]

cur.execute("SELECT COUNT(DISTINCT indicator_code) FROM worldbank_data")
indicators_count = cur.fetchone()[0]

print(f"\n{'='*50}")
print(f"DONE!")
print(f"Total records in database: {count:,}")
print(f"Countries with data: {countries_count}")
print(f"Indicators with data: {indicators_count}")
print(f"{'='*50}")

cur.close()
conn.close()
