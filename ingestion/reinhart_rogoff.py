"""Standalone financial crisis data module for Open Data Platform."""
import pandas as pd

class CrisisData:
    """Financial crisis data provider with sample data fallback."""
    
    COUNTRIES = {
        "USA": "United States", "GBR": "United Kingdom", "DEU": "Germany", "FRA": "France",
        "JPN": "Japan", "ARG": "Argentina", "MEX": "Mexico", "GRC": "Greece", "ESP": "Spain",
        "IRL": "Ireland", "ISL": "Iceland", "TUR": "Turkey", "KOR": "South Korea",
        "THA": "Thailand", "IDN": "Indonesia", "RUS": "Russia", "ZWE": "Zimbabwe",
        "VEN": "Venezuela", "HUN": "Hungary", "SWE": "Sweden", "FIN": "Finland",
    }
    
    CRISIS_TYPES = ["BANKING", "CURRENCY", "SOVEREIGN", "INFLATION", "STOCK_MARKET", "DEBASEMENT"]
    SOURCES = ["LAEVEN_VALENCIA", "REINHART_ROGOFF"]
    
    SAMPLE_DATA = [
        {"country_iso3": "USA", "country": "United States", "crisis_type": "BANKING", "start_year": 2007, "end_year": 2009,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 31.0, "fiscal_cost_pct": 4.5,
         "description": "Subprime mortgage crisis leading to global financial crisis"},
        {"country_iso3": "USA", "country": "United States", "crisis_type": "BANKING", "start_year": 1929, "end_year": 1933,
         "source": "REINHART_ROGOFF", "output_loss_pct": 45.0, "description": "Great Depression banking crisis"},
        {"country_iso3": "GRC", "country": "Greece", "crisis_type": "SOVEREIGN", "start_year": 2010, "end_year": 2018,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 43.0, "fiscal_cost_pct": 27.3, "haircut_pct": 53.0,
         "external_default": True, "description": "Greek sovereign debt crisis"},
        {"country_iso3": "IRL", "country": "Ireland", "crisis_type": "BANKING", "start_year": 2008, "end_year": 2012,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 106.0, "fiscal_cost_pct": 40.7,
         "description": "Irish banking crisis"},
        {"country_iso3": "ISL", "country": "Iceland", "crisis_type": "BANKING", "start_year": 2008, "end_year": 2011,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 43.0, "fiscal_cost_pct": 44.0,
         "description": "Iceland banking collapse"},
        {"country_iso3": "THA", "country": "Thailand", "crisis_type": "BANKING", "start_year": 1997, "end_year": 2000,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 109.0, "fiscal_cost_pct": 43.8,
         "description": "Thai banking crisis - origin of Asian financial crisis"},
        {"country_iso3": "THA", "country": "Thailand", "crisis_type": "CURRENCY", "start_year": 1997, "end_year": 1998,
         "source": "LAEVEN_VALENCIA", "exchange_rate_depreciation": 50.0, "description": "Thai baht collapse"},
        {"country_iso3": "KOR", "country": "South Korea", "crisis_type": "BANKING", "start_year": 1997, "end_year": 1998,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 50.0, "fiscal_cost_pct": 31.2,
         "description": "Korean banking crisis"},
        {"country_iso3": "IDN", "country": "Indonesia", "crisis_type": "BANKING", "start_year": 1997, "end_year": 2001,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 69.0, "fiscal_cost_pct": 56.8,
         "description": "Indonesian banking crisis"},
        {"country_iso3": "ARG", "country": "Argentina", "crisis_type": "SOVEREIGN", "start_year": 2001, "end_year": 2005,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 42.0, "haircut_pct": 73.0, "external_default": True,
         "description": "Argentine sovereign default"},
        {"country_iso3": "ARG", "country": "Argentina", "crisis_type": "INFLATION", "start_year": 1989, "end_year": 1991,
         "source": "REINHART_ROGOFF", "peak_inflation": 3079.5, "description": "Argentine hyperinflation"},
        {"country_iso3": "RUS", "country": "Russia", "crisis_type": "SOVEREIGN", "start_year": 1998, "end_year": 1999,
         "source": "LAEVEN_VALENCIA", "haircut_pct": 50.0, "external_default": True,
         "description": "Russian sovereign default on GKOs"},
        {"country_iso3": "DEU", "country": "Germany", "crisis_type": "INFLATION", "start_year": 1920, "end_year": 1923,
         "source": "REINHART_ROGOFF", "peak_inflation": 29525000000000.0, "description": "Weimar Republic hyperinflation"},
        {"country_iso3": "ZWE", "country": "Zimbabwe", "crisis_type": "INFLATION", "start_year": 2007, "end_year": 2008,
         "source": "REINHART_ROGOFF", "peak_inflation": 79600000000.0, "description": "Zimbabwe hyperinflation"},
        {"country_iso3": "GBR", "country": "United Kingdom", "crisis_type": "BANKING", "start_year": 2007, "end_year": 2011,
         "source": "LAEVEN_VALENCIA", "output_loss_pct": 25.0, "fiscal_cost_pct": 8.8,
         "description": "UK banking crisis - Northern Rock, RBS"},
    ]
    
    def __init__(self, db_manager=None):
        self.db = db_manager
    
    def get_data(self, crisis_type=None, country=None, source=None, year_start=None, year_end=None):
        if self.db and self.db.is_connected():
            query = "SELECT * FROM financial_crises WHERE 1=1"
            params = {}
            if crisis_type:
                query += " AND crisis_type = :crisis_type"
                params["crisis_type"] = crisis_type
            if country:
                query += " AND country_iso3 = :country"
                params["country"] = country
            if source:
                query += " AND source = :source"
                params["source"] = source
            if year_start:
                query += " AND start_year >= :year_start"
                params["year_start"] = year_start
            if year_end:
                query += " AND start_year <= :year_end"
                params["year_end"] = year_end
            query += " ORDER BY start_year DESC"
            results = self.db.execute_query(query, params)
            if results:
                df = pd.DataFrame(results)
                df['country'] = df['country_iso3'].map(self.COUNTRIES)
                return df
        return self._get_sample_data(crisis_type, country, source, year_start, year_end)
    
    def _get_sample_data(self, crisis_type=None, country=None, source=None, year_start=None, year_end=None):
        df = pd.DataFrame(self.SAMPLE_DATA)
        if crisis_type:
            df = df[df['crisis_type'] == crisis_type]
        if country:
            df = df[df['country_iso3'] == country]
        if source:
            df = df[df['source'] == source]
        if year_start:
            df = df[df['start_year'] >= year_start]
        if year_end:
            df = df[df['start_year'] <= year_end]
        return df.sort_values('start_year', ascending=False)
    
    def get_summary_by_type(self, year_start=None, year_end=None):
        df = self.get_data(year_start=year_start, year_end=year_end)
        if df.empty:
            return pd.DataFrame()
        summary = df.groupby('crisis_type').agg({
            'country_iso3': 'count', 'output_loss_pct': 'mean', 'fiscal_cost_pct': 'mean'
        }).rename(columns={'country_iso3': 'count'}).reset_index()
        return summary.sort_values('count', ascending=False)
    
    def get_summary_by_decade(self, crisis_type=None):
        df = self.get_data(crisis_type=crisis_type)
        if df.empty:
            return pd.DataFrame()
        df['decade'] = (df['start_year'] // 10) * 10
        return df.groupby('decade').agg({'country_iso3': 'count'}).rename(columns={'country_iso3': 'count'}).reset_index().sort_values('decade')
    
    def get_twin_crises(self):
        df = self.get_data()
        if df.empty:
            return pd.DataFrame()
        banking = df[df['crisis_type'] == 'BANKING'][['country_iso3', 'country', 'start_year', 'description']].rename(columns={'description': 'banking_description'})
        currency = df[df['crisis_type'] == 'CURRENCY'][['country_iso3', 'start_year', 'description']].rename(columns={'description': 'currency_description'})
        return banking.merge(currency, on=['country_iso3', 'start_year'])
    
    def get_countries(self):
        df = self.get_data()
        return df[['country_iso3', 'country']].drop_duplicates().to_dict('records')
    
    def get_crisis_types(self):
        return self.CRISIS_TYPES
    
    def get_sources(self):
        return self.SOURCES
