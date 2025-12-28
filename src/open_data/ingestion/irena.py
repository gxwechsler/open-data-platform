
from datetime import datetime
from decimal import Decimal
import httpx
import pandas as pd
from open_data.config import COUNTRIES, DataSource
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source
from open_data.ingestion.base import BaseCollector, IngestionResult

IRENA_API_BASE = "https://pxweb.irena.org/api/v1/en/IRENASTAT"
IRENA_INDICATORS = {
    "IRENA.CAP.RENEW": "Renewable electricity capacity (MW)",
    "IRENA.CAP.SOLAR": "Solar PV capacity (MW)",
    "IRENA.CAP.WIND": "Wind capacity (MW)",
    "IRENA.CAP.HYDRO": "Hydropower capacity (MW)",
    "IRENA.GEN.RENEW": "Renewable electricity generation (GWh)",
    "IRENA.GEN.SOLAR": "Solar PV generation (GWh)",
    "IRENA.GEN.WIND": "Wind generation (GWh)",
    "IRENA.GEN.HYDRO": "Hydropower generation (GWh)",
}
TECH_MAP = {"0": "RENEW", "1": "SOLAR", "3": "WIND", "5": "HYDRO"}

class IRENACollector(BaseCollector):
    source_code = DataSource.IRENA
    source_name = "International Renewable Energy Agency"
    base_url = IRENA_API_BASE
    def __init__(self, countries=None, start_year=2000, end_year=None, indicators=None):
        super().__init__(countries, start_year, end_year)
        self.indicator_codes = indicators or list(IRENA_INDICATORS.keys())
        self.client = httpx.Client(timeout=120)
    def fetch_indicators(self):
        return [{"code": c, "name": n} for c, n in IRENA_INDICATORS.items()]
    def fetch_data(self, indicators, countries=None):
        url = f"{IRENA_API_BASE}/Power Capacity and Generation/Country_ELECSTAT_2025_H2_PX.px"
        target_countries = countries or list(COUNTRIES.keys())
        year_indices = [str(y - 2000) for y in range(self.start_year, self.end_year + 1)]
        records = []
        for data_type, type_name in [("0", "CAP"), ("1", "GEN")]:
            print(f"  Fetching {type_name} data...")
            query = {"query": [
                {"code": "Country/area", "selection": {"filter": "item", "values": target_countries}},
                {"code": "Technology", "selection": {"filter": "item", "values": list(TECH_MAP.keys())}},
                {"code": "Data Type", "selection": {"filter": "item", "values": [data_type]}},
                {"code": "Grid connection", "selection": {"filter": "item", "values": ["0"]}},
                {"code": "Year", "selection": {"filter": "item", "values": year_indices}}
            ], "response": {"format": "json"}}
            try:
                r = self.client.post(url, json=query, timeout=120)
                if r.status_code == 200:
                    data = r.json().get("data", [])
                    print(f"    Got {len(data)} records")
                    for item in data:
                        country, tech, _, _, year_idx = item["key"]
                        value = item["values"][0]
                        if value and value not in ("-", "", "0") and tech in TECH_MAP:
                            try:
                                val = float(value)
                                indicator = f"IRENA.{type_name}.{TECH_MAP[tech]}"
                                records.append({"country": country, "year": 2000 + int(year_idx), "indicator": indicator, "value": val})
                            except ValueError:
                                pass
                else:
                    print(f"    Error: {r.status_code}")
            except Exception as e:
                print(f"    Error: {e}")
        return pd.DataFrame(records) if records else pd.DataFrame(columns=["country", "year", "indicator", "value"])
    def _get_or_create_indicator(self, session, source, code, name):
        ind = session.query(Indicator).filter_by(source_id=source.id, code=code).first()
        if not ind:
            cat = session.query(Category).filter_by(code="ENERGY").first() or session.query(Category).filter_by(code="ENVIRONMENT").first()
            ind = Indicator(source_id=source.id, category_id=cat.id if cat else None, code=code, name=name[:255], frequency="annual")
            session.add(ind); session.flush()
        return ind
    def _get_country_map(self, session):
        return {c.iso3_code: c.id for c in session.query(Country).all()}
    def collect(self, indicators=None, countries=None):
        result = IngestionResult(source=self.source_code.value, started_at=datetime.utcnow())
        try:
            with session_scope() as session:
                source = self.get_or_create_source(session)
                log = self.create_ingestion_log(session, source)
                country_map = self._get_country_map(session)
                indicator_map = {code: self._get_or_create_indicator(session, source, code, IRENA_INDICATORS.get(code, code)).id for code in IRENA_INDICATORS.keys()}
                print("Fetching IRENA renewable energy data...")
                df = self.fetch_data(list(IRENA_INDICATORS.keys()), countries or list(COUNTRIES.keys()))
                if df.empty: result.status = "completed"; result.completed_at = datetime.utcnow(); return result
                print(f"Total fetched: {len(df)}")
                saved = 0
                for _, row in df.iterrows():
                    cid, iid = country_map.get(row["country"]), indicator_map.get(row["indicator"])
                    if cid and iid:
                        ex = session.query(Observation).filter_by(country_id=cid, indicator_id=iid, year=int(row["year"])).first()
                        if ex: ex.value = Decimal(str(row["value"])); ex.fetched_at = datetime.utcnow()
                        else: session.add(Observation(country_id=cid, indicator_id=iid, year=int(row["year"]), value=Decimal(str(row["value"])), is_estimated=False, fetched_at=datetime.utcnow()))
                        saved += 1
                result.records_processed = saved
                print(f"Saved: {saved}")
                source.last_updated = datetime.utcnow()
                result.status = "completed"; result.completed_at = datetime.utcnow()
                self.update_ingestion_log(session, log, result)
        except Exception as e: result.status = "failed"; result.errors.append(str(e)); result.completed_at = datetime.utcnow(); import traceback; traceback.print_exc()
        return result
