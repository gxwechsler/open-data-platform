"""
Uppsala Conflict Data Program (UCDP) API client.

UCDP is the world's main provider of data on organized violence.
API Documentation: https://ucdp.uu.se/apidocs/
"""

import time
from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import requests
from sqlalchemy.orm import Session

from open_data.config import (
    COUNTRIES,
    UCDP_API_BASE,
    UCDP_API_VERSION,
    UCDP_INDICATORS,
    DataSource,
)
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source
from open_data.ingestion.base import BaseCollector, IngestionResult


# Mapping from UCDP location IDs to ISO3 codes
# UCDP uses Gleditsch-Ward country codes, we need to map to ISO3
UCDP_TO_ISO3 = {
    "United States of America": "USA",
    "United Kingdom": "GBR",
    "Germany": "DEU",
    "France": "FRA",
    "Italy": "ITA",
    "Spain": "ESP",
    "Turkey": "TUR",
    "Russia": "RUS",
    "China": "CHN",
    "India": "IND",
    "Japan": "JPN",
    "Brazil": "BRA",
    "Mexico": "MEX",
    "Argentina": "ARG",
    "Colombia": "COL",
    "Chile": "CHL",
    "Canada": "CAN",
    "Australia": "AUS",
    "South Africa": "ZAF",
    "Egypt": "EGY",
    "Nigeria": "NGA",
    "Algeria": "DZA",
    "Morocco": "MAR",
    "Ethiopia": "ETH",
    "DR Congo (Zaire)": "COD",
    "Congo, Democratic Republic of": "COD",
    "Tanzania": "TZA",
    "Iran": "IRN",
    "Saudi Arabia": "SAU",
    "Israel": "ISR",
    "United Arab Emirates": "ARE",
    "Qatar": "QAT",
    "Vietnam": "VNM",
    "Singapore": "SGP",
    "Sweden": "SWE",
    "Norway": "NOR",
    "Denmark": "DNK",
    "Finland": "FIN",
    "Netherlands": "NLD",
    "Switzerland": "CHE",
    "New Zealand": "NZL",
    "Libya": "LBY",
    "Tunisia": "TUN",
    "Ghana": "GHA",
    "Niger": "NER",
}


class UCDPCollector(BaseCollector):
    """Collector for Uppsala Conflict Data Program."""

    source_code = DataSource.UCDP
    source_name = "Uppsala Conflict Data Program"
    base_url = UCDP_API_BASE

    def __init__(
        self,
        countries: list[str] | None = None,
        start_year: int = 1989,  # UCDP data starts in 1989
        end_year: int | None = None,
        indicators: list[str] | None = None,
    ):
        """
        Initialize UCDP collector.

        Args:
            countries: List of ISO3 country codes.
            start_year: First year to collect (UCDP starts at 1989).
            end_year: Last year to collect.
            indicators: List of UCDP indicator codes.
        """
        # Ensure years are integers
        start_year = int(start_year) if start_year else 1989
        start_year = max(start_year, 1989)
        if end_year is None:
            end_year = datetime.now().year
        else:
            end_year = int(end_year)
        super().__init__(countries, start_year, end_year)
        self.indicator_codes = indicators or list(UCDP_INDICATORS.keys())
        self.session = requests.Session()
        self.session.headers.update({"Accept": "application/json"})

    def _make_request(self, endpoint: str, params: dict | None = None) -> dict:
        """Make a request to the UCDP API."""
        url = f"{UCDP_API_BASE}/{endpoint}/{UCDP_API_VERSION}"
        params = params or {}
        params["pagesize"] = 1000

        all_results = []
        page = 0

        while True:
            params["page"] = page
            response = self.session.get(url, params=params, timeout=60)
            response.raise_for_status()
            data = response.json()

            results = data.get("Result", [])
            if not results:
                break

            all_results.extend(results)
            page += 1

            # Respect rate limits
            time.sleep(0.2)

            # Safety limit
            if page > 100:
                break

        return all_results

    def fetch_indicators(self) -> list[dict[str, Any]]:
        """Fetch indicator metadata."""
        return [
            {
                "code": code,
                "name": name,
                "description": f"UCDP {name}",
                "unit": "deaths",
                "source": "UCDP",
            }
            for code, name in UCDP_INDICATORS.items()
        ]

    def _fetch_battle_deaths(self) -> pd.DataFrame:
        """Fetch battle-related deaths from state-based conflicts."""
        print("  Fetching battle-related deaths...")
        try:
            data = self._make_request("battledeaths")
        except Exception as e:
            print(f"  Warning: Could not fetch battle deaths: {e}")
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        records = []
        for row in data:
            location = row.get("location", "")
            iso3 = self._location_to_iso3(location)
            if not iso3 or iso3 not in self.countries:
                continue

            try:
                year = int(row.get("year", 0))
            except (ValueError, TypeError):
                continue
            if not (self.start_year <= year <= self.end_year):
                continue

            bd_best = row.get("bd_best", 0) or 0
            bd_low = row.get("bd_low", 0) or 0
            bd_high = row.get("bd_high", 0) or 0

            records.append({
                "country": iso3,
                "year": year,
                "UCDP.BD.TOTAL": bd_best,
                "UCDP.BD.LOW": bd_low,
                "UCDP.BD.HIGH": bd_high,
            })

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        # Aggregate by country-year (sum all conflicts)
        df = df.groupby(["country", "year"]).sum().reset_index()
        return df

    def _fetch_nonstate_deaths(self) -> pd.DataFrame:
        """Fetch deaths from non-state conflicts."""
        print("  Fetching non-state conflict deaths...")
        try:
            data = self._make_request("nonstate")
        except Exception as e:
            print(f"  Warning: Could not fetch non-state deaths: {e}")
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        records = []
        for row in data:
            location = row.get("location", "")
            iso3 = self._location_to_iso3(location)
            if not iso3 or iso3 not in self.countries:
                continue

            try:
                year = int(row.get("year", 0))
            except (ValueError, TypeError):
                continue
            if not (self.start_year <= year <= self.end_year):
                continue

            best = row.get("best_fatality_estimate", 0) or 0
            low = row.get("low_fatality_estimate", 0) or 0
            high = row.get("high_fatality_estimate", 0) or 0

            records.append({
                "country": iso3,
                "year": year,
                "UCDP.NS.TOTAL": best,
                "UCDP.NS.LOW": low,
                "UCDP.NS.HIGH": high,
            })

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df = df.groupby(["country", "year"]).sum().reset_index()
        return df

    def _fetch_onesided_deaths(self) -> pd.DataFrame:
        """Fetch deaths from one-sided violence (attacks on civilians)."""
        print("  Fetching one-sided violence deaths...")
        try:
            data = self._make_request("onesided")
        except Exception as e:
            print(f"  Warning: Could not fetch one-sided deaths: {e}")
            return pd.DataFrame()

        if not data:
            return pd.DataFrame()

        records = []
        for row in data:
            location = row.get("location", "")
            iso3 = self._location_to_iso3(location)
            if not iso3 or iso3 not in self.countries:
                continue

            try:
                year = int(row.get("year", 0))
            except (ValueError, TypeError):
                continue
            if not (self.start_year <= year <= self.end_year):
                continue

            best = row.get("best_fatality_estimate", 0) or 0
            low = row.get("low_fatality_estimate", 0) or 0
            high = row.get("high_fatality_estimate", 0) or 0

            records.append({
                "country": iso3,
                "year": year,
                "UCDP.OS.TOTAL": best,
                "UCDP.OS.LOW": low,
                "UCDP.OS.HIGH": high,
            })

        if not records:
            return pd.DataFrame()

        df = pd.DataFrame(records)
        df = df.groupby(["country", "year"]).sum().reset_index()
        return df

    def _location_to_iso3(self, location: str) -> str | None:
        """Convert UCDP location name to ISO3 code."""
        # Direct mapping
        if location in UCDP_TO_ISO3:
            return UCDP_TO_ISO3[location]

        # Try to match by country name in our config
        for iso3, country in COUNTRIES.items():
            if country.name.lower() == location.lower():
                return iso3
            if location.lower() in country.name.lower():
                return iso3

        return None

    def fetch_data(
        self,
        indicators: list[str],
        countries: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch conflict data for specified indicators and countries.

        Args:
            indicators: List of UCDP indicator codes.
            countries: List of ISO3 country codes.

        Returns:
            DataFrame with columns: country, indicator, year, value
        """
        target_countries = countries or self.countries

        all_data = []

        # Fetch data based on requested indicators
        bd_indicators = [i for i in indicators if i.startswith("UCDP.BD")]
        ns_indicators = [i for i in indicators if i.startswith("UCDP.NS")]
        os_indicators = [i for i in indicators if i.startswith("UCDP.OS")]

        if bd_indicators:
            df = self._fetch_battle_deaths()
            if not df.empty:
                all_data.append(df)

        if ns_indicators:
            df = self._fetch_nonstate_deaths()
            if not df.empty:
                all_data.append(df)

        if os_indicators:
            df = self._fetch_onesided_deaths()
            if not df.empty:
                all_data.append(df)

        if not all_data:
            return pd.DataFrame(columns=["country", "indicator", "year", "value"])

        # Merge all dataframes
        result = all_data[0]
        for df in all_data[1:]:
            result = pd.merge(result, df, on=["country", "year"], how="outer")

        # Melt to long format
        id_vars = ["country", "year"]
        value_vars = [c for c in result.columns if c.startswith("UCDP.")]
        
        result = result.melt(
            id_vars=id_vars,
            value_vars=value_vars,
            var_name="indicator",
            value_name="value",
        )

        # Filter to requested indicators
        result = result[result["indicator"].isin(indicators)]

        # Filter to target countries
        result = result[result["country"].isin(target_countries)]

        # Remove null/zero values - ensure value is numeric
        result["value"] = pd.to_numeric(result["value"], errors="coerce")
        result = result.dropna(subset=["value"])
        result = result[result["value"] > 0]

        return result

    def _get_or_create_indicator(
        self,
        session: Session,
        source: Source,
        code: str,
        name: str,
    ) -> Indicator:
        """Get or create an indicator record."""
        indicator = (
            session.query(Indicator)
            .filter_by(source_id=source.id, code=code)
            .first()
        )
        if not indicator:
            # Use SECURITY category for conflict data
            category = session.query(Category).filter_by(code="SECURITY").first()

            indicator = Indicator(
                source_id=source.id,
                category_id=category.id if category else None,
                code=code,
                name=name[:255],
                unit="deaths",
                frequency="annual",
            )
            session.add(indicator)
            session.flush()
        return indicator

    def _get_country_map(self, session: Session) -> dict[str, int]:
        """Get mapping of ISO3 codes to country IDs."""
        countries = session.query(Country).all()
        return {c.iso3_code: c.id for c in countries}

    def collect(
        self,
        indicators: list[str] | None = None,
        countries: list[str] | None = None,
    ) -> IngestionResult:
        """Run the full UCDP data collection."""
        result = IngestionResult(
            source=self.source_code.value,
            started_at=datetime.utcnow(),
        )

        indicator_codes = indicators or self.indicator_codes
        target_countries = countries or self.countries

        try:
            with session_scope() as session:
                # Get source
                source = self.get_or_create_source(session)
                log = self.create_ingestion_log(session, source)

                # Get country ID mapping
                country_map = self._get_country_map(session)

                # Create/update indicator records
                indicator_map: dict[str, int] = {}
                for code in indicator_codes:
                    name = UCDP_INDICATORS.get(code, code)
                    ind = self._get_or_create_indicator(session, source, code, name)
                    indicator_map[code] = ind.id

                # Fetch all data
                print("Fetching UCDP conflict data...")
                df = self.fetch_data(indicator_codes, target_countries)

                if df.empty:
                    print("No conflict data found for specified countries/years")
                    result.status = "completed"
                    result.completed_at = datetime.utcnow()
                    return result

                print(f"Total records fetched: {len(df)}")

                # Prepare records for insertion
                records_saved = 0
                for _, row in df.iterrows():
                    country_id = country_map.get(row["country"])
                    indicator_id = indicator_map.get(row["indicator"])

                    if country_id and indicator_id:
                        # Check if record exists
                        existing = (
                            session.query(Observation)
                            .filter_by(
                                country_id=country_id,
                                indicator_id=indicator_id,
                                year=int(row["year"]),
                            )
                            .first()
                        )

                        if existing:
                            existing.value = Decimal(str(row["value"]))
                            existing.fetched_at = datetime.utcnow()
                        else:
                            obs = Observation(
                                country_id=country_id,
                                indicator_id=indicator_id,
                                year=int(row["year"]),
                                value=Decimal(str(row["value"])),
                                is_estimated=False,
                                fetched_at=datetime.utcnow(),
                            )
                            session.add(obs)

                        records_saved += 1
                        result.records_processed += 1

                # Update source timestamp
                source.last_updated = datetime.utcnow()

                result.status = "completed"
                result.completed_at = datetime.utcnow()

                self.update_ingestion_log(session, log, result)

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            result.completed_at = datetime.utcnow()
            import traceback
            traceback.print_exc()

        return result


def fetch_conflict_summary(
    countries: list[str] | None = None,
    start_year: int = 1989,
    end_year: int | None = None,
) -> pd.DataFrame:
    """
    Convenience function to fetch a summary of conflict deaths.

    Args:
        countries: List of ISO3 codes.
        start_year: First year.
        end_year: Last year.

    Returns:
        DataFrame with conflict death summary.
    """
    collector = UCDPCollector(
        countries=countries,
        start_year=start_year,
        end_year=end_year,
    )
    return collector.fetch_data(list(UCDP_INDICATORS.keys()), countries)
