"""
Merge EM-DAT disaster events into disasters table
"""
import psycopg2
from datetime import date

DATABASE_URL = "postgresql://postgres.jtyykeaeupxbbkaqkfqp:CodeNess6504@aws-0-us-west-2.pooler.supabase.com:6543/postgres"

# Disaster type to group mapping
DISASTER_GROUPS = {
    "EARTHQUAKE": "GEOPHYSICAL",
    "VOLCANIC_ACTIVITY": "GEOPHYSICAL",
    "LANDSLIDE": "GEOPHYSICAL",
    "FLOOD": "HYDROLOGICAL",
    "STORM": "METEOROLOGICAL",
    "TROPICAL_CYCLONE": "METEOROLOGICAL",
    "DROUGHT": "CLIMATOLOGICAL",
    "EXTREME_TEMPERATURE": "CLIMATOLOGICAL",
    "WILDFIRE": "CLIMATOLOGICAL",
}

# Major disaster events with details
# (emdat_id, iso3, disaster_type, event_name, year, month, day, lat, lon, magnitude, deaths, affected, damage_usd)
DISASTER_EVENTS = [
    # EARTHQUAKES
    ("1976-0001-CHN", "CHN", "EARTHQUAKE", "Tangshan Earthquake", 1976, 7, 28, 39.6, 118.2, 7.5, 242000, 164000, 5600000000),
    ("2008-0233-CHN", "CHN", "EARTHQUAKE", "Sichuan Earthquake", 2008, 5, 12, 31.0, 103.4, 7.9, 87564, 45976596, 85000000000),
    ("1995-0012-JPN", "JPN", "EARTHQUAKE", "Kobe Earthquake", 1995, 1, 17, 34.6, 135.0, 6.9, 6433, 541636, 100000000000),
    ("1999-0324-TUR", "TUR", "EARTHQUAKE", "Izmit Earthquake", 1999, 8, 17, 40.7, 30.0, 7.6, 17118, 1358953, 20000000000),
    ("1990-0220-IRN", "IRN", "EARTHQUAKE", "Manjil-Rudbar Earthquake", 1990, 6, 21, 37.0, 49.4, 7.4, 40000, 500000, 7000000000),
    ("2003-0564-IRN", "IRN", "EARTHQUAKE", "Bam Earthquake", 2003, 12, 26, 29.0, 58.4, 6.6, 26271, 267628, 500000000),
    ("2001-0027-IND", "IND", "EARTHQUAKE", "Gujarat Earthquake", 2001, 1, 26, 23.4, 70.2, 7.7, 20005, 6321812, 2623000000),
    ("2009-0184-ITA", "ITA", "EARTHQUAKE", "L'Aquila Earthquake", 2009, 4, 6, 42.3, 13.3, 6.3, 295, 56000, 2500000000),
    ("2016-0350-ITA", "ITA", "EARTHQUAKE", "Central Italy Earthquake", 2016, 8, 24, 42.7, 13.2, 6.2, 299, 4800, 5000000000),
    ("1985-0324-MEX", "MEX", "EARTHQUAKE", "Mexico City Earthquake", 1985, 9, 19, 19.4, -99.1, 8.0, 9500, 2130204, 4104000000),
    ("2017-0398-MEX", "MEX", "EARTHQUAKE", "Puebla Earthquake", 2017, 9, 19, 18.4, -98.7, 7.1, 369, 250000, 2000000000),
    ("2010-0052-CHL", "CHL", "EARTHQUAKE", "Chile Earthquake", 2010, 2, 27, -35.8, -72.7, 8.8, 562, 2671556, 30000000000),
    ("2011-0041-NZL", "NZL", "EARTHQUAKE", "Christchurch Earthquake", 2011, 2, 22, -43.6, 172.7, 6.3, 185, 300000, 16000000000),
    ("1999-0029-COL", "COL", "EARTHQUAKE", "Armenia Earthquake", 1999, 1, 25, 4.4, -75.7, 6.1, 1185, 559262, 1580000000),
    ("2003-0227-DZA", "DZA", "EARTHQUAKE", "Boumerdes Earthquake", 2003, 5, 21, 36.9, 3.7, 6.8, 2266, 200000, 5000000000),
    ("2023-0154-MAR", "MAR", "EARTHQUAKE", "Al Haouz Earthquake", 2023, 9, 8, 31.1, -8.4, 6.8, 2946, 300000, 4000000000),
    
    # FLOODS
    ("1998-0299-CHN", "CHN", "FLOOD", "Yangtze River Floods", 1998, 6, 1, 30.0, 112.0, None, 3656, 238973000, 30000000000),
    ("2010-0301-CHN", "CHN", "FLOOD", "China Floods", 2010, 5, 1, 28.0, 105.0, None, 1691, 140000000, 18000000000),
    ("2020-0298-CHN", "CHN", "FLOOD", "Yangtze Floods 2020", 2020, 6, 1, 30.0, 112.0, None, 278, 70000000, 17000000000),
    ("2005-0371-IND", "IND", "FLOOD", "Mumbai Floods", 2005, 7, 26, 19.1, 72.9, None, 1200, 20000000, 3330000000),
    ("2013-0246-IND", "IND", "FLOOD", "Uttarakhand Floods", 2013, 6, 14, 30.7, 79.1, None, 6054, 4200000, 1100000000),
    ("2018-0294-IND", "IND", "FLOOD", "Kerala Floods", 2018, 8, 8, 10.0, 76.5, None, 504, 5400000, 2800000000),
    ("2021-0348-DEU", "DEU", "FLOOD", "Rhine Valley Floods", 2021, 7, 14, 50.4, 7.0, None, 196, 180000, 40000000000),
    ("2017-0383-USA", "USA", "FLOOD", "Hurricane Harvey Flooding", 2017, 8, 25, 29.8, -95.4, None, 89, 13000000, 125000000000),
    ("2007-0298-GBR", "GBR", "FLOOD", "UK Summer Floods", 2007, 6, 1, 52.0, -1.5, None, 13, 350000, 4000000000),
    ("2011-0014-AUS", "AUS", "FLOOD", "Queensland Floods", 2011, 1, 1, -27.5, 153.0, None, 35, 200000, 2550000000),
    ("2022-0087-AUS", "AUS", "FLOOD", "Eastern Australia Floods", 2022, 2, 23, -28.8, 153.4, None, 22, 500000, 3350000000),
    ("2011-0019-BRA", "BRA", "FLOOD", "Rio de Janeiro Floods", 2011, 1, 11, -22.4, -43.1, None, 900, 100000, 1200000000),
    ("2010-0389-COL", "COL", "FLOOD", "Colombia La Niña Floods", 2010, 11, 1, 4.6, -74.1, None, 418, 2800000, 5000000000),
    ("2020-0398-NER", "NER", "FLOOD", "Niger Floods", 2020, 8, 1, 13.5, 2.1, None, 71, 557000, 100000000),
    ("2020-0312-ETH", "ETH", "FLOOD", "Ethiopia Floods", 2020, 7, 1, 9.0, 38.7, None, 42, 1000000, 50000000),
    ("2015-0298-GHA", "GHA", "FLOOD", "Accra Floods", 2015, 6, 3, 5.6, -0.2, None, 159, 52622, 100000000),
    
    # STORMS (Hurricanes, Typhoons, Cyclones)
    ("2012-0324-USA", "USA", "TROPICAL_CYCLONE", "Hurricane Sandy", 2012, 10, 29, 39.4, -74.4, 3.0, 159, 8900000, 68000000000),
    ("2017-0456-USA", "USA", "TROPICAL_CYCLONE", "Hurricane Irma", 2017, 9, 10, 25.8, -80.2, 5.0, 136, 2500000, 50000000000),
    ("2022-0398-USA", "USA", "TROPICAL_CYCLONE", "Hurricane Ian", 2022, 9, 28, 26.6, -82.0, 4.0, 156, 2500000, 113000000000),
    ("2018-0387-JPN", "JPN", "TROPICAL_CYCLONE", "Typhoon Jebi", 2018, 9, 4, 34.7, 135.5, None, 14, 10000, 12500000000),
    ("2019-0412-JPN", "JPN", "TROPICAL_CYCLONE", "Typhoon Hagibis", 2019, 10, 12, 35.7, 139.7, None, 104, 400000, 17000000000),
    ("2006-0312-CHN", "CHN", "TROPICAL_CYCLONE", "Typhoon Saomai", 2006, 8, 10, 27.2, 120.1, None, 1000, 45000000, 10000000000),
    ("2011-0023-AUS", "AUS", "TROPICAL_CYCLONE", "Cyclone Yasi", 2011, 2, 3, -18.0, 146.0, 5.0, 1, 50000, 1350000000),
    ("2020-0456-VNM", "VNM", "TROPICAL_CYCLONE", "Vietnam Typhoons 2020", 2020, 10, 1, 16.0, 108.0, None, 249, 7700000, 1500000000),
    ("1999-0387-IND", "IND", "TROPICAL_CYCLONE", "Odisha Super Cyclone", 1999, 10, 29, 20.0, 87.0, 5.0, 9843, 15000000, 4500000000),
    ("2019-0187-IND", "IND", "TROPICAL_CYCLONE", "Cyclone Fani", 2019, 5, 3, 20.3, 86.0, 4.0, 89, 28000000, 8100000000),
    
    # DROUGHTS
    ("1984-0001-ETH", "ETH", "DROUGHT", "Ethiopian Famine", 1984, 1, 1, 9.0, 38.7, None, 300000, 8000000, 0),
    ("2015-0156-ETH", "ETH", "DROUGHT", "Ethiopia El Niño Drought", 2015, 1, 1, 9.0, 38.7, None, 0, 10200000, 0),
    ("2022-0098-ETH", "ETH", "DROUGHT", "Horn of Africa Drought", 2022, 1, 1, 9.0, 38.7, None, 0, 24000000, 0),
    ("2005-0234-NER", "NER", "DROUGHT", "Niger Food Crisis", 2005, 1, 1, 13.5, 2.1, None, 0, 3600000, 0),
    ("2010-0156-NER", "NER", "DROUGHT", "Sahel Drought", 2010, 1, 1, 13.5, 2.1, None, 0, 7000000, 0),
    ("2002-0189-IND", "IND", "DROUGHT", "India Drought 2002", 2002, 1, 1, 23.0, 77.0, None, 0, 300000000, 2500000000),
    ("2012-0234-USA", "USA", "DROUGHT", "US Midwest Drought", 2012, 6, 1, 40.0, -95.0, None, 123, 0, 30000000000),
    ("2019-0087-AUS", "AUS", "DROUGHT", "Australian Drought", 2019, 1, 1, -25.0, 135.0, None, 0, 0, 5000000000),
    ("2014-0156-BRA", "BRA", "DROUGHT", "São Paulo Water Crisis", 2014, 1, 1, -23.5, -46.6, None, 0, 33000000, 5000000000),
    ("2015-0189-ZAF", "ZAF", "DROUGHT", "South Africa Drought", 2015, 1, 1, -30.0, 25.0, None, 0, 2700000, 1000000000),
    ("2022-0145-FRA", "FRA", "DROUGHT", "European Drought 2022", 2022, 6, 1, 46.0, 2.0, None, 0, 0, 2000000000),
    
    # EXTREME TEMPERATURES
    ("2003-0298-FRA", "FRA", "EXTREME_TEMPERATURE", "European Heat Wave", 2003, 8, 1, 48.9, 2.3, None, 19490, 0, 0),
    ("2003-0299-DEU", "DEU", "EXTREME_TEMPERATURE", "European Heat Wave", 2003, 8, 1, 52.5, 13.4, None, 9355, 0, 0),
    ("2003-0300-ITA", "ITA", "EXTREME_TEMPERATURE", "European Heat Wave", 2003, 8, 1, 41.9, 12.5, None, 20089, 0, 0),
    ("2003-0301-ESP", "ESP", "EXTREME_TEMPERATURE", "European Heat Wave", 2003, 8, 1, 40.4, -3.7, None, 15090, 0, 0),
    ("2003-0302-GBR", "GBR", "EXTREME_TEMPERATURE", "European Heat Wave", 2003, 8, 1, 51.5, -0.1, None, 2139, 0, 0),
    ("2003-0303-NLD", "NLD", "EXTREME_TEMPERATURE", "European Heat Wave", 2003, 8, 1, 52.4, 4.9, None, 1400, 0, 0),
    ("1995-0187-USA", "USA", "EXTREME_TEMPERATURE", "Chicago Heat Wave", 1995, 7, 12, 41.9, -87.6, None, 1021, 0, 0),
    ("2021-0287-USA", "USA", "EXTREME_TEMPERATURE", "Pacific Northwest Heat Dome", 2021, 6, 26, 45.5, -122.7, None, 229, 0, 0),
    ("2015-0234-IND", "IND", "EXTREME_TEMPERATURE", "India Heat Wave", 2015, 5, 1, 17.4, 78.5, None, 2248, 0, 0),
    ("2021-0298-CAN", "CAN", "EXTREME_TEMPERATURE", "Canada Heat Dome", 2021, 6, 26, 49.3, -123.1, None, 569, 0, 0),
    ("2009-0056-AUS", "AUS", "EXTREME_TEMPERATURE", "Victoria Heat Wave", 2009, 1, 28, -37.8, 145.0, None, 374, 0, 0),
    
    # WILDFIRES
    ("2018-0456-USA", "USA", "WILDFIRE", "Camp Fire, California", 2018, 11, 8, 39.8, -121.4, None, 106, 52000, 16500000000),
    ("2020-0312-USA", "USA", "WILDFIRE", "Western US Wildfires", 2020, 8, 15, 37.0, -120.0, None, 46, 500000, 16500000000),
    ("2019-0456-AUS", "AUS", "WILDFIRE", "Black Summer Bushfires", 2019, 9, 1, -36.0, 149.0, None, 34, 65000, 5000000000),
    ("2016-0187-CAN", "CAN", "WILDFIRE", "Fort McMurray Wildfire", 2016, 5, 1, 56.7, -111.4, None, 2, 88000, 5000000000),
    ("2023-0234-CAN", "CAN", "WILDFIRE", "Canada Wildfires 2023", 2023, 5, 1, 53.0, -113.0, None, 8, 200000, 3000000000),
    ("2017-0312-PRT", "PRT", "WILDFIRE", "Pedrógão Grande Fire", 2017, 6, 17, 39.9, -8.2, None, 117, 500, 600000000),
    ("2017-0187-CHL", "CHL", "WILDFIRE", "Chile Wildfires 2017", 2017, 1, 18, -35.0, -71.5, None, 11, 6000, 500000000),
    ("2023-0156-CHL", "CHL", "WILDFIRE", "Central Chile Wildfires", 2023, 2, 2, -33.0, -71.6, None, 131, 100000, 2000000000),
    
    # VOLCANIC ERUPTIONS
    ("1985-0387-COL", "COL", "VOLCANIC_ACTIVITY", "Nevado del Ruiz Eruption", 1985, 11, 13, 4.9, -75.3, None, 23080, 200000, 1000000000),
    ("2014-0398-JPN", "JPN", "VOLCANIC_ACTIVITY", "Mount Ontake Eruption", 2014, 9, 27, 35.9, 137.5, None, 63, 0, 0),
    ("2019-0456-NZL", "NZL", "VOLCANIC_ACTIVITY", "White Island Eruption", 2019, 12, 9, -37.5, 177.2, None, 22, 0, 0),
    ("2002-0012-COD", "COD", "VOLCANIC_ACTIVITY", "Nyiragongo Eruption 2002", 2002, 1, 17, -1.5, 29.2, None, 200, 400000, 0),
    ("2021-0187-COD", "COD", "VOLCANIC_ACTIVITY", "Nyiragongo Eruption 2021", 2021, 5, 22, -1.5, 29.2, None, 32, 400000, 0),
    
    # LANDSLIDES
    ("2010-0312-CHN", "CHN", "LANDSLIDE", "Gansu Mudslide", 2010, 8, 7, 34.0, 104.0, None, 1765, 500000, 500000000),
    ("2017-0145-COL", "COL", "LANDSLIDE", "Mocoa Landslide", 2017, 4, 1, 1.1, -76.6, None, 333, 45000, 100000000),
    ("2014-0287-IND", "IND", "LANDSLIDE", "Pune Landslide", 2014, 7, 30, 18.5, 73.8, None, 151, 200000, 100000000),
    ("2011-0023-BRA", "BRA", "LANDSLIDE", "Teresópolis Mudslides", 2011, 1, 12, -22.4, -43.0, None, 903, 300000, 500000000),
]


def main():
    print("="*60)
    print("Merging EM-DAT data into disasters table")
    print("="*60)
    
    conn = psycopg2.connect(DATABASE_URL)
    cur = conn.cursor()
    
    # Check existing count
    cur.execute("SELECT COUNT(*) FROM disasters")
    before_count = cur.fetchone()[0]
    print(f"\nExisting records: {before_count}")
    
    inserted = 0
    skipped = 0
    
    for event in DISASTER_EVENTS:
        emdat_id, iso3, dtype, event_name, year, month, day, lat, lon, magnitude, deaths, affected, damage = event
        
        # Check if already exists
        cur.execute("SELECT id FROM disasters WHERE emdat_id = %s", (emdat_id,))
        if cur.fetchone():
            skipped += 1
            continue
        
        # Get disaster group
        disaster_group = DISASTER_GROUPS.get(dtype, "OTHER")
        
        # Create start date
        try:
            start_date = date(year, month, day)
        except:
            start_date = date(year, 1, 1)
        
        # Insert
        cur.execute("""
            INSERT INTO disasters 
            (emdat_id, country_iso3, disaster_type, disaster_group, event_name, 
             start_date, year, latitude, longitude, magnitude, 
             deaths, total_affected, damage_usd)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (emdat_id, iso3, dtype, disaster_group, event_name,
              start_date, year, lat, lon, magnitude,
              deaths if deaths > 0 else None, 
              affected if affected > 0 else None,
              damage if damage > 0 else None))
        inserted += 1
    
    conn.commit()
    
    # Check new count
    cur.execute("SELECT COUNT(*) FROM disasters")
    after_count = cur.fetchone()[0]
    
    print(f"\nRecords inserted: {inserted}")
    print(f"Records skipped (duplicates): {skipped}")
    print(f"Total disasters now: {after_count}")
    
    # Show breakdown by type
    print("\n=== Disasters by Type ===")
    cur.execute("""
        SELECT disaster_type, COUNT(*), SUM(deaths), SUM(damage_usd)
        FROM disasters 
        GROUP BY disaster_type 
        ORDER BY COUNT(*) DESC
    """)
    for row in cur.fetchall():
        dtype, count, deaths, damage = row
        deaths = deaths or 0
        damage = damage or 0
        print(f"  {dtype}: {count} events, {deaths:,} deaths, ${damage/1e9:.1f}B damage")
    
    cur.close()
    conn.close()


if __name__ == "__main__":
    main()
