"""Standalone FRED data module for Open Data Platform."""
import pandas as pd
import numpy as np
from datetime import date

class FREDData:
    """FRED data provider with sample data fallback."""
    
    SERIES_INFO = {
        "GDP": {"name": "Gross Domestic Product", "category": "National Accounts", "units": "Billions of Dollars", "frequency": "Quarterly"},
        "GDPC1": {"name": "Real Gross Domestic Product", "category": "National Accounts", "units": "Billions of Chained 2017 Dollars", "frequency": "Quarterly"},
        "UNRATE": {"name": "Unemployment Rate", "category": "Labor Market", "units": "Percent", "frequency": "Monthly"},
        "CPIAUCSL": {"name": "Consumer Price Index", "category": "Prices", "units": "Index 1982-1984=100", "frequency": "Monthly"},
        "FEDFUNDS": {"name": "Federal Funds Effective Rate", "category": "Interest Rates", "units": "Percent", "frequency": "Monthly"},
        "DGS10": {"name": "10-Year Treasury Rate", "category": "Interest Rates", "units": "Percent", "frequency": "Monthly"},
        "M2SL": {"name": "M2 Money Stock", "category": "Money Supply", "units": "Billions of Dollars", "frequency": "Monthly"},
        "SP500": {"name": "S&P 500 Index", "category": "Stock Market", "units": "Index", "frequency": "Monthly"},
        "HOUST": {"name": "Housing Starts", "category": "Housing", "units": "Thousands of Units", "frequency": "Monthly"},
        "PAYEMS": {"name": "Total Nonfarm Payrolls", "category": "Labor Market", "units": "Thousands of Persons", "frequency": "Monthly"},
    }
    
    def __init__(self, db_manager=None):
        self.db = db_manager
        self._cache = {}
    
    @classmethod
    def get_available_series(cls):
        return [{"series_id": k, **v} for k, v in cls.SERIES_INFO.items()]
    
    @classmethod
    def get_categories(cls):
        return sorted(set(v["category"] for v in cls.SERIES_INFO.values()))
    
    def get_series(self, series_id, start_date=None, end_date=None):
        if self.db and self.db.is_connected():
            query = "SELECT date, value, series_name, units FROM fed_series WHERE series_id = :series_id"
            params = {"series_id": series_id}
            if start_date:
                query += " AND date >= :start_date"
                params["start_date"] = start_date
            if end_date:
                query += " AND date <= :end_date"
                params["end_date"] = end_date
            query += " ORDER BY date"
            results = self.db.execute_query(query, params)
            if results:
                df = pd.DataFrame(results)
                df['date'] = pd.to_datetime(df['date'])
                return df
        return self._generate_sample_series(series_id, start_date, end_date)
    
    def _generate_sample_series(self, series_id, start_date=None, end_date=None):
        if series_id not in self.SERIES_INFO:
            return pd.DataFrame()
        
        np.random.seed(hash(series_id) % 2**32)
        info = self.SERIES_INFO[series_id]
        
        if info["frequency"] == "Quarterly":
            dates = pd.date_range(start="2000-01-01", end="2024-12-31", freq="QS")
        else:
            dates = pd.date_range(start="2000-01-01", end="2024-12-31", freq="MS")
        
        n = len(dates)
        
        if series_id == "GDP":
            base, trend = 10000, np.cumprod(1 + np.random.normal(0.006, 0.005, n))
            values = base * trend
        elif series_id == "GDPC1":
            base, trend = 12000, np.cumprod(1 + np.random.normal(0.005, 0.004, n))
            values = base * trend
        elif series_id == "UNRATE":
            values = np.zeros(n)
            values[0] = 5.0
            for i in range(1, n):
                values[i] = max(3, min(12, 0.95 * values[i-1] + 0.05 * 5.0 + np.random.normal(0, 0.3)))
        elif series_id == "CPIAUCSL":
            base, trend = 170, np.cumprod(1 + np.random.normal(0.002, 0.001, n))
            values = base * trend
        elif series_id in ["FEDFUNDS", "DGS10"]:
            base = 5.0
            trend = np.linspace(0, -2, n)
            cycle = 0.5 * np.sin(np.linspace(0, 10, n))
            values = np.maximum(0.1, base + trend + cycle + np.random.normal(0, 0.2, n))
        elif series_id == "M2SL":
            base, trend = 4700, np.cumprod(1 + np.random.normal(0.005, 0.002, n))
            values = base * trend
        elif series_id == "SP500":
            base, trend = 1400, np.cumprod(1 + np.random.normal(0.007, 0.04, n))
            values = base * trend
        elif series_id == "HOUST":
            base, cycle = 1500, 400 * np.sin(np.linspace(0, 4, n))
            values = np.maximum(400, base + cycle + np.random.normal(0, 100, n))
        elif series_id == "PAYEMS":
            base, trend = 130000, np.cumprod(1 + np.random.normal(0.001, 0.002, n))
            values = base * trend
        else:
            values = np.random.normal(100, 10, n)
        
        df = pd.DataFrame({"date": dates, "value": np.round(values, 2), "series_name": info["name"], "units": info["units"]})
        
        if start_date:
            df = df[df["date"] >= pd.Timestamp(start_date)]
        if end_date:
            df = df[df["date"] <= pd.Timestamp(end_date)]
        return df
    
    def get_multiple_series(self, series_ids, start_date=None, end_date=None):
        dfs = []
        for sid in series_ids:
            df = self.get_series(sid, start_date, end_date)
            if not df.empty:
                df = df[["date", "value"]].rename(columns={"value": sid})
                df.set_index("date", inplace=True)
                dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        result = dfs[0]
        for df in dfs[1:]:
            result = result.join(df, how="outer")
        return result.reset_index()
