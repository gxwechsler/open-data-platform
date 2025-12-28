
from datetime import datetime
from decimal import Decimal
import httpx
import pandas as pd
from open_data.config import COUNTRIES, DataSource
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source
from open_data.ingestion.base import BaseCollector, IngestionResult

UNHCR_API_BASE = "https://api.unhcr.org/population/v1"
UNHCR_INDICATORS = {"UNHCR.REF.IN": "Refugees hosted", "UNHCR.ASY.IN": "Asylum seekers hosted", "UNHCR.IDP": "Internally displaced", "UNHCR.STA": "Stateless persons", "UNHCR.REF.OUT": "Refugees from country", "UNHCR.ASY.OUT": "Asylum seekers from country"}

class UNHCRCollector(BaseCollector):
    source_code = DataSource.UNHCR
    source_name = "UN High Commissioner for Refugees"
    base_url = UNHCR_API_BASE
    def __init__(self, countries=None, start_year=2000, end_year=None, indicators=None):
        super().__init__(countries, start_year, end_year)
        self.indicator_codes = indicators or list(UNHCR_INDICATORS.keys())
        self.client = httpx.Client(timeout=60)
    def fetch_indicators(self):
        return [{"code": c, "name": n} for c, n in UNHCR_INDICATORS.items()]
    def _fetch_country_data(self, iso3, year):
        records = []
        try:
            r = self.client.get(f"{UNHCR_API_BASE}/population/?year={year}&coa={iso3}")
            if r.status_code == 200:
                for item in r.json().get("items", []):
                    if item.get("refugees"): records.append({"country": iso3, "year": year, "indicator": "UNHCR.REF.IN", "value": item["refugees"]})
                    if item.get("asylum_seekers"): records.append({"country": iso3, "year": year, "indicator": "UNHCR.ASY.IN", "value": item["asylum_seekers"]})
                    if item.get("idps"): records.append({"country": iso3, "year": year, "indicator": "UNHCR.IDP", "value": item["idps"]})
                    if item.get("stateless"): records.append({"country": iso3, "year": year, "indicator": "UNHCR.STA", "value": item["stateless"]})
        except: pass
        try:
            r = self.client.get(f"{UNHCR_API_BASE}/population/?year={year}&coo={iso3}")
            if r.status_code == 200:
                for item in r.json().get("items", []):
                    if item.get("refugees"): records.append({"country": iso3, "year": year, "indicator": "UNHCR.REF.OUT", "value": item["refugees"]})
                    if item.get("asylum_seekers"): records.append({"country": iso3, "year": year, "indicator": "UNHCR.ASY.OUT", "value": item["asylum_seekers"]})
        except: pass
        return records
    def fetch_data(self, indicators, countries=None):
        all_records = []
        for year in range(self.start_year, self.end_year + 1):
            print(f"  {year}...")
            for iso3 in (countries or list(COUNTRIES.keys())):
                all_records.extend([r for r in self._fetch_country_data(iso3, year) if r["indicator"] in indicators])
        if not all_records: return pd.DataFrame(columns=["country", "year", "indicator", "value"])
        return pd.DataFrame(all_records).groupby(["country", "year", "indicator"])["value"].sum().reset_index()
    def _get_or_create_indicator(self, session, source, code, name):
        ind = session.query(Indicator).filter_by(source_id=source.id, code=code).first()
        if not ind:
            cat = session.query(Category).filter_by(code="HUMANITARIAN").first() or session.query(Category).filter_by(code="SOCIAL").first()
            ind = Indicator(source_id=source.id, category_id=cat.id if cat else None, code=code, name=name[:255], frequency="annual")
            session.add(ind); session.flush()
        return ind
    def _get_country_map(self, session):
        return {c.iso3_code: c.id for c in session.query(Country).all()}
    def collect(self, indicators=None, countries=None):
        result = IngestionResult(source=self.source_code.value, started_at=datetime.utcnow())
        indicator_codes = indicators or self.indicator_codes
        try:
            with session_scope() as session:
                source = self.get_or_create_source(session)
                log = self.create_ingestion_log(session, source)
                country_map = self._get_country_map(session)
                indicator_map = {code: self._get_or_create_indicator(session, source, code, UNHCR_INDICATORS.get(code, code)).id for code in indicator_codes}
                print("Fetching UNHCR data...")
                df = self.fetch_data(indicator_codes, countries or self.countries)
                if df.empty: result.status = "completed"; result.completed_at = datetime.utcnow(); return result
                print(f"Records: {len(df)}")
                for _, row in df.iterrows():
                    cid, iid = country_map.get(row["country"]), indicator_map.get(row["indicator"])
                    if cid and iid:
                        ex = session.query(Observation).filter_by(country_id=cid, indicator_id=iid, year=int(row["year"])).first()
                        if ex: ex.value = Decimal(str(row["value"])); ex.fetched_at = datetime.utcnow()
                        else: session.add(Observation(country_id=cid, indicator_id=iid, year=int(row["year"]), value=Decimal(str(row["value"])), is_estimated=False, fetched_at=datetime.utcnow()))
                        result.records_processed += 1
                source.last_updated = datetime.utcnow()
                result.status = "completed"; result.completed_at = datetime.utcnow()
                self.update_ingestion_log(session, log, result)
        except Exception as e: result.status = "failed"; result.errors.append(str(e)); result.completed_at = datetime.utcnow()
        return result
