import psycopg2
import requests
import time

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

COUNTRIES = "ARG;BRA;CHL;COL;MEX;USA;CAN;DEU;FRA;ITA;SWE;NLD;CHE;DNK;FIN;NOR;TUR;ESP;GBR;IRL;IND;CHN;JPN;VNM;SGP;ISR;IRN;ARE;SAU;QAT;NER;ZAF;EGY;COD;MAR;DZA;ETH;LBY;TZA;TUN;GHA;AUS;NZL"

INDICATORS = {
    "NY.GDP.MKTP.CD": ("GDP (current US$)", "Economy"),
    "NY.GDP.MKTP.KD.ZG": ("GDP growth (annual %)", "Economy"),
    "NY.GDP.PCAP.CD": ("GDP per capita (current US$)", "Economy"),
    "NY.GDP.PCAP.KD.ZG": ("GDP per capita growth (annual %)", "Economy"),
    "NY.GNP.MKTP.CD": ("GNI (current US$)", "Economy"),
    "NY.GNP.PCAP.CD": ("GNI per capita (current US$)", "Economy"),
    "NE.EXP.GNFS.ZS": ("Exports of goods and services (% of GDP)", "Economy"),
    "NE.IMP.GNFS.ZS": ("Imports of goods and services (% of GDP)", "Economy"),
    "NE.TRD.GNFS.ZS": ("Trade (% of GDP)", "Economy"),
    "SL.UEM.TOTL.ZS": ("Unemployment, total (% of labor force)", "Labor"),
    "SL.UEM.TOTL.FE.ZS": ("Unemployment, female (% of labor force)", "Labor"),
    "SL.UEM.TOTL.MA.ZS": ("Unemployment, male (% of labor force)", "Labor"),
    "SL.TLF.CACT.ZS": ("Labor force participation rate, total (%)", "Labor"),
    "SL.TLF.CACT.FE.ZS": ("Labor force participation rate, female (%)", "Labor"),
    "SL.EMP.TOTL.SP.ZS": ("Employment to population ratio (%)", "Labor"),
    "FP.CPI.TOTL.ZG": ("Inflation, consumer prices (annual %)", "Prices"),
    "FP.CPI.TOTL": ("Consumer price index (2010 = 100)", "Prices"),
    "NY.GDP.DEFL.KD.ZG": ("Inflation, GDP deflator (annual %)", "Prices"),
    "SP.POP.TOTL": ("Population, total", "Population"),
    "SP.POP.GROW": ("Population growth (annual %)", "Population"),
    "SP.URB.TOTL.IN.ZS": ("Urban population (% of total)", "Population"),
    "SP.POP.65UP.TO.ZS": ("Population ages 65+ (% of total)", "Population"),
    "SP.POP.0014.TO.ZS": ("Population ages 0-14 (% of total)", "Population"),
    "SP.DYN.CBRT.IN": ("Birth rate, crude (per 1,000 people)", "Population"),
    "SP.DYN.CDRT.IN": ("Death rate, crude (per 1,000 people)", "Population"),
    "SI.POV.DDAY": ("Poverty headcount ratio at $2.15/day (%)", "Poverty"),
    "SI.POV.LMIC": ("Poverty headcount ratio at $3.65/day (%)", "Poverty"),
    "SI.POV.GINI": ("Gini index", "Poverty"),
    "SI.DST.10TH.10": ("Income share held by highest 10%", "Poverty"),
    "SI.DST.FRST.10": ("Income share held by lowest 10%", "Poverty"),
    "SP.DYN.LE00.IN": ("Life expectancy at birth (years)", "Health"),
    "SP.DYN.LE00.FE.IN": ("Life expectancy at birth, female (years)", "Health"),
    "SP.DYN.LE00.MA.IN": ("Life expectancy at birth, male (years)", "Health"),
    "SH.DYN.MORT": ("Mortality rate, under-5 (per 1,000 live births)", "Health"),
    "SH.DYN.NMRT": ("Mortality rate, neonatal (per 1,000 live births)", "Health"),
    "SP.DYN.IMRT.IN": ("Mortality rate, infant (per 1,000 live births)", "Health"),
    "SH.XPD.CHEX.GD.ZS": ("Current health expenditure (% of GDP)", "Health"),
    "SH.MED.PHYS.ZS": ("Physicians (per 1,000 people)", "Health"),
    "SH.MED.BEDS.ZS": ("Hospital beds (per 1,000 people)", "Health"),
    "SE.ADT.LITR.ZS": ("Literacy rate, adult total (%)", "Education"),
    "SE.PRM.ENRR": ("School enrollment, primary (% gross)", "Education"),
    "SE.SEC.ENRR": ("School enrollment, secondary (% gross)", "Education"),
    "SE.TER.ENRR": ("School enrollment, tertiary (% gross)", "Education"),
    "SE.XPD.TOTL.GD.ZS": ("Government expenditure on education (% of GDP)", "Education"),
    "SE.PRM.CMPT.ZS": ("Primary completion rate (%)", "Education"),
    "IT.NET.USER.ZS": ("Individuals using the Internet (%)", "Infrastructure"),
    "IT.CEL.SETS.P2": ("Mobile cellular subscriptions (per 100 people)", "Infrastructure"),
    "EG.ELC.ACCS.ZS": ("Access to electricity (% of population)", "Infrastructure"),
    "EG.USE.ELEC.KH.PC": ("Electric power consumption (kWh per capita)", "Infrastructure"),
    "IS.AIR.PSGR": ("Air transport, passengers carried", "Infrastructure"),
    "EN.ATM.CO2E.PC": ("CO2 emissions (metric tons per capita)", "Environment"),
    "EN.ATM.CO2E.KT": ("CO2 emissions (kt)", "Environment"),
    "AG.LND.FRST.ZS": ("Forest area (% of land area)", "Environment"),
    "EG.FEC.RNEW.ZS": ("Renewable energy consumption (%)", "Environment"),
    "EN.ATM.PM25.MC.M3": ("PM2.5 air pollution (micrograms per cubic meter)", "Environment"),
    "FR.INR.RINR": ("Real interest rate (%)", "Finance"),
    "FS.AST.DOMS.GD.ZS": ("Domestic credit provided by financial sector (% of GDP)", "Finance"),
    "BX.KLT.DINV.WD.GD.ZS": ("Foreign direct investment, net inflows (% of GDP)", "Finance"),
    "GC.DOD.TOTL.GD.ZS": ("Central government debt, total (% of GDP)", "Finance"),
    "BN.CAB.XOKA.GD.ZS": ("Current account balance (% of GDP)", "Finance"),
}

def fetch_indicator(indicator_code, countries, start_year, end_year):
    url = f"https://api.worldbank.org/v2/country/{countries}/indicator/{indicator_code}"
    params = {"format": "json", "date": f"{start_year}:{end_year}", "per_page": 10000}
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

print(f"\nFetching historical data (1970-1999) for {len(INDICATORS)} indicators...")
print("This will take several minutes...\n")

total_inserted = 0
for i, (code, (name, category)) in enumerate(INDICATORS.items(), 1):
    print(f"[{i}/{len(INDICATORS)}] {name}...")
    records = fetch_indicator(code, COUNTRIES, 1970, 1999)
    
    for r in records:
        try:
            cur.execute("""
                INSERT INTO unified_indicators (source, country_iso3, country_name, indicator_code, indicator_name, category, year, value)
                VALUES ('WB', %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (source, country_iso3, indicator_code, year) DO NOTHING
            """, (r[0], r[1], r[2], r[3], category, r[4], r[5]))
        except Exception as e:
            pass
    
    conn.commit()
    total_inserted += len(records)
    print(f"    -> {len(records)} records")
    time.sleep(0.5)

cur.execute("SELECT COUNT(*) FROM unified_indicators")
total = cur.fetchone()[0]

cur.execute("SELECT MIN(year), MAX(year) FROM unified_indicators")
years = cur.fetchone()

print(f"\n{'='*50}")
print(f"DONE!")
print(f"New records added: {total_inserted}")
print(f"Total records in database: {total}")
print(f"Year range: {years[0]} - {years[1]}")
print(f"{'='*50}")

cur.close()
conn.close()
