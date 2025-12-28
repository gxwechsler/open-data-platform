"""
IMF DataMapper API client.
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
import pandas as pd
from sqlalchemy.orm import Session

from open_data.config import COUNTRIES, COUNTRY_CODES, DataSource
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source
from open_data.ingestion.base import BaseCollector, IngestionResult


IMF_DATAMAPPER_BASE = "https://www.imf.org/external/datamapper/api/v1"

IMF_DATAMAPPER_INDICATORS = {
    "NGDP_RPCH": "Real GDP growth (annual %)",
    "NGDPD": "GDP, current prices (billions USD)",
    "NGDPDPC": "GDP per capita, current prices (USD)",
    "PPPGDP": "GDP, PPP (billions international $)",
    "PPPPC": "GDP per capita, PPP (international $)",
    "PCPIPCH": "Inflation rate, average consumer prices (%)",
    "PCPIEPCH": "Inflation rate, end of period (%)",
    "LP": "Population (millions)",
    "LUR": "Unemployment rate (%)",
    "BCA": "Current account balance (billions USD)",
    "BCA_NGDPD": "Current account balance (% of GDP)",
    "GGXWDG_NGDP": "General government gross debt (% of GDP)",
}


class IMFDataMapperCollector(BaseCollector):
    source_code = DataSource.IMF
    source_name = "International Monetary Fund"
    base_url = IMF_DATAMAPPER_BASE

    def __init__(self, countries=None, start_year=1980, end_year=None, indicators=None):
        super().__init__(countries, start_year, end_year)
        self.indicator_codes = indicators or list(IMF_DATAMAPPER_INDICATORS.keys())
        self.client = httpx.Client(timeout=60)

    def fetch_indicators(self):
        return [{"code": c, "name": n} for c, n in IMF_DATAMAPPER_INDICATORS.items()]

    def _fetch_indicator_data(self, indicator):
        url = f"{IMF_DATAMAPPER_BASE}/{indicator}"
        try:
            response = self.client.get(url)
            response.raise_for_status()
            data = response.json()
        except Exception as e:
            print(f"    Error fetching {indicator}: {e}")
            return pd.DataFrame()

        values = data.get("values", {}).get(indicator, {})
        if not values:
            return pd.DataFrame()

        records = []
        for country_code, yearly_data in values.items():
            if country_code not in COUNTRIES or country_code not in self.countries:
                continue
            for year_str, value in yearly_data.items():
                try:
                    year = int(year_str)
                    if self.start_year <= year <= self.end_year and value is not None:
                        records.append({"country": country_code, "indicator": indicator, "year": year, "value": float(value)})
                except (ValueError, TypeError):
                    continue
        return pd.DataFrame(records)

    def fetch_data(self, indicators, countries=None):
        all_data = []
        for indicator in indicators:
            print(f"  Fetching {indicator}...")
            df = self._fetch_indicator_data(indicator)
            if not df.empty:
                all_data.append(df)
        if not all_data:
            return pd.DataFrame(columns=["country", "indicator", "year", "value"])
        return pd.concat(all_data, ignore_index=True)

    def _get_or_create_indicator(self, session, source, code, name):
        indicator = session.query(Indicator).filter_by(source_id=source.id, code=code).first()
        if not indicator:
            category = session.query(Category).filter_by(code="FINANCIAL").first()
            if not category:
                category = session.query(Category).filter_by(code="ECONOMIC").first()
            indicator = Indicator(source_id=source.id, category_id=category.id if category else None, code=code, name=name[:255], frequency="annual")
            session.add(indicator)
            session.flush()
        return indicator

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

                indicator_map = {}
                for code in indicator_codes:
                    name = IMF_DATAMAPPER_INDICATORS.get(code, code)
                    ind = self._get_or_create_indicator(session, source, code, name)
                    indicator_map[code] = ind.id

                print("Fetching IMF DataMapper data...")
                df = self.fetch_data(indicator_codes)

                if df.empty:
                    result.status = "completed"
                    result.completed_at = datetime.utcnow()
                    return result

                print(f"Total records fetched: {len(df)}")

                for _, row in df.iterrows():
                    country_id = country_map.get(row["country"])
                    indicator_id = indicator_map.get(row["indicator"])
                    if country_id and indicator_id:
                        existing = session.query(Observation).filter_by(country_id=country_id, indicator_id=indicator_id, year=int(row["year"])).first()
                        if existing:
                            existing.value = Decimal(str(row["value"]))
                            existing.fetched_at = datetime.utcnow()
                        else:
                            session.add(Observation(country_id=country_id, indicator_id=indicator_id, year=int(row["year"]), value=Decimal(str(row["value"])), is_estimated=False, fetched_at=datetime.utcnow()))
                        result.records_processed += 1

                source.last_updated = datetime.utcnow()
                result.status = "completed"
                result.completed_at = datetime.utcnow()
                self.update_ingestion_log(session, log, result)

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            result.completed_at = datetime.utcnow()

        return result
