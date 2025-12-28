import wbgapi as wb
from sqlalchemy import select
from open_data.db.connection import get_engine
from open_data.db.models import Country, Indicator, Source, Observation, Category
from sqlalchemy.orm import Session
from datetime import datetime

print("=" * 70)
print("COMPREHENSIVE DATA LOADING SCRIPT")
print("=" * 70)

# Configuration
COUNTRIES = ['ARG', 'BRA', 'CHL', 'MEX', 'USA', 'CHN', 'IND', 'DEU', 'GBR', 'JPN']
YEARS = range(2015, 2024)  # 2015-2023
INDICATORS = {
    'NY.GDP.MKTP.CD': ('GDP (current US$)', 'Economic', 'USD'),
    'NY.GDP.PCAP.CD': ('GDP per capita (current US$)', 'Economic', 'USD'),
    'NY.GDP.MKTP.KD.ZG': ('GDP growth (annual %)', 'Economic', '%'),
    'FP.CPI.TOTL.ZG': ('Inflation, consumer prices (annual %)', 'Economic', '%'),
    'SL.UEM.TOTL.ZS': ('Unemployment, total (% of labor force)', 'Economic', '%'),
    'SP.POP.TOTL': ('Population, total', 'Demographic', 'persons'),
}

# ISO2 code mapping (some are non-standard)
ISO2_MAP = {
    'ARG': 'AR', 'BRA': 'BR', 'CHL': 'CL', 'MEX': 'MX', 'USA': 'US',
    'CHN': 'CN', 'IND': 'IN', 'DEU': 'DE', 'GBR': 'GB', 'JPN': 'JP'
}

engine = get_engine()
session = Session(engine)

try:
    # 1. Ensure World Bank source exists
    stmt = select(Source).where(Source.code == 'WB')
    source = session.execute(stmt).scalar_one_or_none()
    if not source:
        source = Source(code='WB', name='World Bank', base_url='https://api.worldbank.org')
        session.add(source)
        session.commit()
    print(f"\n✓ Source: {source.name}")

    # 2. Get/Create categories
    stmt = select(Category).where(Category.code == 'ECONOMIC')
    econ_cat = session.execute(stmt).scalar_one_or_none()
    if not econ_cat:
        econ_cat = Category(code='ECONOMIC', name='Economic')
        session.add(econ_cat)
        session.commit()
    
    stmt = select(Category).where(Category.code == 'DEMOGRAPHIC')
    demo_cat = session.execute(stmt).scalar_one_or_none()
    if not demo_cat:
        demo_cat = Category(code='DEMOGRAPHIC', name='Demographic')
        session.add(demo_cat)
        session.commit()

    # 3. Ensure indicators exist
    print(f"\n✓ Loading {len(INDICATORS)} indicators...")
    indicator_map = {}
    for ind_code, (ind_name, ind_category, ind_unit) in INDICATORS.items():
        stmt = select(Indicator).where(
            Indicator.code == ind_code,
            Indicator.source_id == source.id
        )
        indicator = session.execute(stmt).scalar_one_or_none()
        if not indicator:
            cat_id = econ_cat.id if ind_category == 'Economic' else demo_cat.id
            indicator = Indicator(
                code=ind_code,
                source_id=source.id,
                category_id=cat_id,
                name=ind_name,
                unit=ind_unit
            )
            session.add(indicator)
            session.commit()
        indicator_map[ind_code] = indicator
        print(f"  - {ind_code}: {ind_name}")

    # 4. Load country metadata
    print(f"\n✓ Loading countries...")
    wb_countries = wb.economy.DataFrame()
    
    country_map = {}
    for country_code in COUNTRIES:
        stmt = select(Country).where(Country.iso3_code == country_code)
        country = session.execute(stmt).scalar_one_or_none()
        
        if not country:
            country_info = wb_countries[wb_countries.index == country_code]
            if not country_info.empty:
                country_name = country_info['name'].values[0]
                print(f"  - Creating: {country_code} ({country_name})")
                
                region = 'AMERICA' if country_code in ['ARG', 'BRA', 'CHL', 'MEX', 'USA'] else \
                        'ASIA' if country_code in ['CHN', 'IND', 'JPN'] else \
                        'EUROPE'
                
                country = Country(
                    iso3_code=country_code,
                    iso2_code=ISO2_MAP[country_code],  # Use correct ISO2 code
                    name=country_name,
                    region=region
                )
                session.add(country)
                session.commit()
        
        country_map[country_code] = country
        print(f"  ✓ {country_code}: {country.name}")

    # 5. Load data for each indicator
    print(f"\n{'=' * 70}")
    print("LOADING DATA FROM WORLD BANK API")
    print(f"{'=' * 70}")
    
    total_saved = 0
    for ind_code, (ind_name, _, _) in INDICATORS.items():
        print(f"\n📊 {ind_name} ({ind_code})")
        print(f"   Countries: {len(COUNTRIES)}, Years: {min(YEARS)}-{max(YEARS)}")
        
        try:
            data = wb.data.DataFrame(
                ind_code, 
                COUNTRIES, 
                time=YEARS,
                columns='series',
                skipBlanks=True
            )
            
            saved_count = 0
            for year_str in data.index.get_level_values('time').unique():
                year = int(year_str.replace('YR', ''))
                
                for country_code in COUNTRIES:
                    try:
                        value = data.loc[(country_code, year_str), ind_code]
                        
                        if value and str(value) != 'nan':
                            obs = Observation(
                                country_id=country_map[country_code].id,
                                indicator_id=indicator_map[ind_code].id,
                                year=year,
                                value=float(value)
                            )
                            session.add(obs)
                            saved_count += 1
                    except KeyError:
                        continue
            
            session.commit()
            total_saved += saved_count
            print(f"   ✓ Saved {saved_count} observations")
            
        except Exception as e:
            print(f"   ✗ Error: {e}")
            session.rollback()
            continue

    print(f"\n{'=' * 70}")
    print(f"✓ LOADING COMPLETE!")
    print(f"{'=' * 70}")
    print(f"Total observations saved: {total_saved:,}")
    
    print("\n📊 DATABASE SUMMARY:")
    print(f"  Sources:      {len(session.execute(select(Source)).all())}")
    print(f"  Countries:    {len(session.execute(select(Country)).all())}")
    print(f"  Indicators:   {len(session.execute(select(Indicator)).all())}")
    print(f"  Observations: {len(session.execute(select(Observation)).all()):,}")

except Exception as e:
    print(f"\n✗ Fatal Error: {e}")
    import traceback
    traceback.print_exc()
    session.rollback()
finally:
    session.close()

print("\n" + "=" * 70)
print("Done! Try: opendata db status")
print("=" * 70)
