"""Standalone disaster data module for Open Data Platform."""
import pandas as pd
from datetime import date

class DisasterData:
    """Disaster data provider with sample data fallback."""
    
    COUNTRIES = {
        "USA": "United States", "JPN": "Japan", "CHN": "China", "IND": "India",
        "IDN": "Indonesia", "PHL": "Philippines", "THA": "Thailand", "PAK": "Pakistan",
        "BGD": "Bangladesh", "HTI": "Haiti", "NPL": "Nepal", "MMR": "Myanmar",
        "AUS": "Australia", "FRA": "France", "RUS": "Russia", "TUR": "Turkey",
    }
    
    SAMPLE_DATA = [
        {"emdat_id": "2010-0016-HTI", "country_iso3": "HTI", "country": "Haiti", "disaster_type": "EARTHQUAKE", 
         "disaster_group": "GEOPHYSICAL", "event_name": "Haiti Earthquake", "year": 2010, 
         "start_date": date(2010, 1, 12), "magnitude": 7.0, "deaths": 222570, "total_affected": 3700000, 
         "damage_usd": 8000000000, "latitude": 18.45, "longitude": -72.45},
        {"emdat_id": "2011-0077-JPN", "country_iso3": "JPN", "country": "Japan", "disaster_type": "EARTHQUAKE",
         "disaster_group": "GEOPHYSICAL", "event_name": "Tohoku Earthquake and Tsunami", "year": 2011,
         "start_date": date(2011, 3, 11), "magnitude": 9.1, "deaths": 19846, "total_affected": 368820,
         "damage_usd": 210000000000, "latitude": 38.3, "longitude": 142.4},
        {"emdat_id": "2005-0324-USA", "country_iso3": "USA", "country": "United States", "disaster_type": "TROPICAL_CYCLONE",
         "disaster_group": "METEOROLOGICAL", "event_name": "Hurricane Katrina", "year": 2005,
         "start_date": date(2005, 8, 29), "magnitude": 5, "deaths": 1833, "total_affected": 500000,
         "damage_usd": 125000000000, "latitude": 29.95, "longitude": -90.07},
        {"emdat_id": "2013-0405-PHL", "country_iso3": "PHL", "country": "Philippines", "disaster_type": "TROPICAL_CYCLONE",
         "disaster_group": "METEOROLOGICAL", "event_name": "Typhoon Haiyan", "year": 2013,
         "start_date": date(2013, 11, 8), "magnitude": 5, "deaths": 7354, "total_affected": 16100000,
         "damage_usd": 10000000000, "latitude": 11.25, "longitude": 125.0},
        {"emdat_id": "2017-0347-USA", "country_iso3": "USA", "country": "United States", "disaster_type": "TROPICAL_CYCLONE",
         "disaster_group": "METEOROLOGICAL", "event_name": "Hurricane Maria", "year": 2017,
         "start_date": date(2017, 9, 20), "magnitude": 5, "deaths": 2975, "total_affected": 3400000,
         "damage_usd": 90000000000, "latitude": 18.22, "longitude": -65.58},
        {"emdat_id": "2022-0125-PAK", "country_iso3": "PAK", "country": "Pakistan", "disaster_type": "FLOOD",
         "disaster_group": "HYDROLOGICAL", "event_name": "Pakistan Monsoon Floods", "year": 2022,
         "start_date": date(2022, 6, 14), "deaths": 1739, "total_affected": 33000000,
         "damage_usd": 30000000000, "latitude": 27.0, "longitude": 68.0},
        {"emdat_id": "2019-0370-AUS", "country_iso3": "AUS", "country": "Australia", "disaster_type": "WILDFIRE",
         "disaster_group": "CLIMATOLOGICAL", "event_name": "Black Summer Bushfires", "year": 2019,
         "start_date": date(2019, 9, 1), "deaths": 34, "total_affected": 10000000,
         "damage_usd": 100000000000, "latitude": -33.87, "longitude": 151.21},
        {"emdat_id": "2003-0319-FRA", "country_iso3": "FRA", "country": "France", "disaster_type": "HEAT_WAVE",
         "disaster_group": "METEOROLOGICAL", "event_name": "European Heat Wave", "year": 2003,
         "start_date": date(2003, 8, 1), "deaths": 19490, "latitude": 46.6, "longitude": 1.9},
        {"emdat_id": "2004-0560-IDN", "country_iso3": "IDN", "country": "Indonesia", "disaster_type": "TSUNAMI",
         "disaster_group": "GEOPHYSICAL", "event_name": "Indian Ocean Tsunami", "year": 2004,
         "start_date": date(2004, 12, 26), "deaths": 165708, "total_affected": 532898,
         "damage_usd": 10000000000, "latitude": 3.3, "longitude": 95.95},
        {"emdat_id": "2023-0175-TUR", "country_iso3": "TUR", "country": "Turkey", "disaster_type": "EARTHQUAKE",
         "disaster_group": "GEOPHYSICAL", "event_name": "Turkey-Syria Earthquake", "year": 2023,
         "start_date": date(2023, 2, 6), "magnitude": 7.8, "deaths": 50783, "total_affected": 26000000,
         "damage_usd": 34200000000, "latitude": 37.2, "longitude": 37.0},
    ]
    
    DISASTER_TYPES = ["EARTHQUAKE", "TROPICAL_CYCLONE", "FLOOD", "DROUGHT", "WILDFIRE", 
                      "HEAT_WAVE", "TSUNAMI", "VOLCANIC", "EPIDEMIC", "LANDSLIDE"]
    DISASTER_GROUPS = ["GEOPHYSICAL", "METEOROLOGICAL", "HYDROLOGICAL", "CLIMATOLOGICAL", "BIOLOGICAL"]
    
    def __init__(self, db_manager=None):
        self.db = db_manager
    
    def get_data(self, disaster_type=None, disaster_group=None, country=None, year_start=None, year_end=None):
        if self.db and self.db.is_connected():
            query = "SELECT * FROM disasters WHERE 1=1"
            params = {}
            if disaster_type:
                query += " AND disaster_type = :disaster_type"
                params["disaster_type"] = disaster_type
            if disaster_group:
                query += " AND disaster_group = :disaster_group"
                params["disaster_group"] = disaster_group
            if country:
                query += " AND country_iso3 = :country"
                params["country"] = country
            if year_start:
                query += " AND year >= :year_start"
                params["year_start"] = year_start
            if year_end:
                query += " AND year <= :year_end"
                params["year_end"] = year_end
            query += " ORDER BY year DESC, deaths DESC NULLS LAST"
            results = self.db.execute_query(query, params)
            if results:
                df = pd.DataFrame(results)
                df['country'] = df['country_iso3'].map(self.COUNTRIES)
                return df
        return self._get_sample_data(disaster_type, disaster_group, country, year_start, year_end)
    
    def _get_sample_data(self, disaster_type=None, disaster_group=None, country=None, year_start=None, year_end=None):
        df = pd.DataFrame(self.SAMPLE_DATA)
        if disaster_type:
            df = df[df['disaster_type'] == disaster_type]
        if disaster_group:
            df = df[df['disaster_group'] == disaster_group]
        if country:
            df = df[df['country_iso3'] == country]
        if year_start:
            df = df[df['year'] >= year_start]
        if year_end:
            df = df[df['year'] <= year_end]
        return df.sort_values(['year', 'deaths'], ascending=[False, False])
    
    def get_summary_by_year(self, disaster_type=None):
        df = self.get_data(disaster_type=disaster_type)
        if df.empty:
            return pd.DataFrame()
        summary = df.groupby('year').agg({
            'deaths': 'sum', 'total_affected': 'sum', 'damage_usd': 'sum', 'emdat_id': 'count'
        }).rename(columns={'emdat_id': 'event_count'}).reset_index()
        return summary.sort_values('year')
    
    def get_summary_by_type(self, year_start=None, year_end=None):
        df = self.get_data(year_start=year_start, year_end=year_end)
        if df.empty:
            return pd.DataFrame()
        summary = df.groupby('disaster_type').agg({
            'deaths': 'sum', 'total_affected': 'sum', 'damage_usd': 'sum', 'emdat_id': 'count'
        }).rename(columns={'emdat_id': 'event_count'}).reset_index()
        return summary.sort_values('deaths', ascending=False)
    
    def get_countries(self):
        df = self.get_data()
        return df[['country_iso3', 'country']].drop_duplicates().to_dict('records')
    
    def get_disaster_types(self):
        return self.DISASTER_TYPES
    
    def get_disaster_groups(self):
        return self.DISASTER_GROUPS
