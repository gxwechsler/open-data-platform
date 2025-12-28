import wbgapi as wb
from sqlalchemy import select
from open_data.db.connection import get_engine
from open_data.db.models import Country, Indicator, Source, Observation
from sqlalchemy.orm import Session

print("Manual data ingestion test...")

engine = get_engine()
session = Session(engine)

try:
    # Get existing records
    stmt = select(Source).where(Source.code == 'WB')
    source = session.execute(stmt).scalar_one()
    print(f"✓ Source: {source.name}")

    stmt = select(Country).where(Country.iso3_code == 'ARG')
    country = session.execute(stmt).scalar_one()
    print(f"✓ Country: {country.name}")

    stmt = select(Indicator).where(
        Indicator.code == 'NY.GDP.MKTP.CD',
        Indicator.source_id == source.id
    )
    indicator = session.execute(stmt).scalar_one()
    print(f"✓ Indicator: {indicator.name}")

    # Fetch data
    print("\nFetching data from World Bank...")
    data = wb.data.DataFrame('NY.GDP.MKTP.CD', 'ARG', time=range(2020, 2024), columns='series')
    
    # Save each year's data
    saved = 0
    for year_str, value in data['NY.GDP.MKTP.CD'].items():
        if str(value) != 'nan':
            year = int(year_str.replace('YR', ''))
            obs = Observation(
                country_id=country.id,
                indicator_id=indicator.id,
                year=year,
                value=float(value)
            )
            session.add(obs)
            print(f"  Saving: {year} = ${value:,.0f}")
            saved += 1
    
    session.commit()
    print(f"\n✓ Saved {saved} observations!")
    
    # Verify
    stmt = select(Observation).order_by(Observation.year)
    all_obs = session.execute(stmt).scalars().all()
    print(f"\n✓ Total observations in database: {len(all_obs)}")
    
    print("\nData in database:")
    for obs in all_obs:
        print(f"  - Year {obs.year}: ${obs.value:,.0f}")

except Exception as e:
    print(f"✗ Error: {e}")
    import traceback
    traceback.print_exc()
    session.rollback()
finally:
    session.close()
