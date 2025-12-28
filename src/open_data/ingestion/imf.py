"""
IMF Data API client.

Uses the IMF's JSON RESTful API (SDMX format).
Documentation: https://datahelp.imf.org/knowledgebase/articles/667681

Key Databases:
- IFS: International Financial Statistics
- BOP: Balance of Payments
- DOT: Direction of Trade Statistics
- GFS: Government Finance Statistics
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import httpx
import pandas as pd
from sqlalchemy.orm import Session

from open_data.config import COUNTRY_CODES, DataSource, settings
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source
from open_data.ingestion.base import BaseCollector, IngestionResult


# IMF country code mapping (IMF uses different codes than ISO3)
# Map from ISO3 to IMF country codes
ISO3_TO_IMF = {
    # AMERICA
    "ARG": "AR", "BRA": "BR", "CHL": "CL", "COL": "CO", "MEX": "MX",
    "USA": "US", "CAN": "CA",
    # EUROPE
    "DEU": "DE", "FRA": "FR", "ITA": "IT", "SWE": "SE", "NLD": "NL",
    "CHE": "CH", "DNK": "DK", "FIN": "FI", "NOR": "NO", "TUR": "TR",
    "ESP": "ES", "GBR": "GB",
    # ASIA
    "IND": "IN", "CHN": "CN", "JPN": "JP", "VNM": "VN", "SGP": "SG",
    # MIDDLE EAST
    "ISR": "IL", "IRN": "IR", "ARE": "AE", "SAU": "SA", "QAT": "QA",
    # AFRICA
    "NER": "NE", "ZAF": "ZA", "EGY": "EG", "COD": "CD", "MAR": "MA",
    "DZA": "DZ", "ETH": "ET", "LBY": "LY", "TZA": "TZ", "TUN": "TN",
    "GHA": "GH",
    # SOUTH PACIFIC
    "AUS": "AU", "NZL": "NZ",
}

IMF_TO_ISO3 = {v: k for k, v in ISO3_TO_IMF.items()}


# Key IMF indicators from International Financial Statistics (IFS)
IMF_INDICATORS = {
    # Exchange Rates
    "ENDA_XDC_USD_RATE": "Exchange Rate, End of Period (LCU per USD)",
    "ENEA_XDC_USD_RATE": "Exchange Rate, Period Average (LCU per USD)",
    # Interest Rates
    "FPOLM_PA": "Monetary Policy Rate (%)",
    "FITB_PA": "Treasury Bill Rate (%)",
    "FILR_PA": "Lending Rate (%)",
    "FIDR_PA": "Deposit Rate (%)",
    # Money and Banking
    "FM_A": "Broad Money (National Currency)",
    "FMB_XDC": "Monetary Base (National Currency)",
    # Prices
    "PCPI_IX": "Consumer Price Index (2010=100)",
    "PCPI_PC_CP_A_PT": "Inflation Rate (%)",
    "PPPI_IX": "Producer Price Index (2010=100)",
    # Balance of Payments
    "BCA_BP6_USD": "Current Account Balance (USD)",
    "BGS_BP6_USD": "Goods and Services Balance (USD)",
    "BXG_BP6_USD": "Exports of Goods (USD)",
    "BMG_BP6_USD": "Imports of Goods (USD)",
    # International Reserves
    "RAFA_USD": "Total Reserves (USD)",
    "RAFAGOLD_USD": "Gold Reserves (USD)",
    # GDP (from IFS)
    "NGDP_XDC": "GDP, Current Prices (National Currency)",
    "NGDP_R_XDC": "GDP, Constant Prices (National Currency)",
    "NGDP_D_IX": "GDP Deflator (Index)",
}

# IMF database codes
IMF_DATABASES = {
    "IFS": "International Financial Statistics",
    "BOP": "Balance of Payments",
    "DOT": "Direction of Trade Statistics",
    "GFS": "Government Finance Statistics",
    "CDIS": "Coordinated Direct Investment Survey",
    "CPIS": "Coordinated Portfolio Investment Survey",
}


class IMFCollector(BaseCollector):
    """Collector for IMF International Financial Statistics."""

    source_code = DataSource.IMF
    source_name = "International Monetary Fund"
    base_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/"

    def __init__(
        self,
        countries: list[str] | None = None,
        start_year: int = 1960,
        end_year: int | None = None,
        indicators: list[str] | None = None,
        database: str = "IFS",
    ):
        """
        Initialize IMF collector.

        Args:
            countries: List of ISO3 country codes.
            start_year: First year to collect.
            end_year: Last year to collect.
            indicators: List of IMF indicator codes.
            database: IMF database code (IFS, BOP, etc.)
        """
        super().__init__(countries, start_year, end_year)
        self.indicator_codes = indicators or list(IMF_INDICATORS.keys())
        self.database = database
        self.client = httpx.Client(timeout=settings.request_timeout)

    def _get_imf_countries(self) -> list[str]:
        """Convert ISO3 codes to IMF country codes."""
        return [ISO3_TO_IMF.get(c, c) for c in self.countries if c in ISO3_TO_IMF]

    def fetch_indicators(self) -> list[dict[str, Any]]:
        """
        Fetch available indicators from IMF.

        Returns:
            List of indicator definitions.
        """
        indicators = []
        for code, name in IMF_INDICATORS.items():
            indicators.append({
                "code": code,
                "name": name,
                "description": "",
                "unit": "",
                "source": "IMF",
                "database": self.database,
            })
        return indicators

    def _fetch_dataflow_structure(self) -> dict:
        """Fetch the structure of the IFS dataflow."""
        url = f"{self.base_url}DataStructure/{self.database}"
        try:
            response = self.client.get(url)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            print(f"Error fetching dataflow structure: {e}")
            return {}

    def fetch_data(
        self,
        indicators: list[str],
        countries: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch data from IMF API.

        The IMF API uses a key-based URL structure:
        /CompactData/{database}/{frequency}.{country}.{indicator}

        Args:
            indicators: List of IMF indicator codes.
            countries: List of country codes (ISO3 or IMF format).

        Returns:
            DataFrame with columns: country, indicator, year, value
        """
        target_countries = countries or self.countries
        imf_countries = [ISO3_TO_IMF.get(c, c) for c in target_countries if c in ISO3_TO_IMF]

        if not imf_countries:
            return pd.DataFrame(columns=["country", "indicator", "year", "value"])

        all_data = []

        for indicator in indicators:
            # Build the dimension key: Frequency.Country.Indicator
            # A = Annual, Q = Quarterly, M = Monthly
            frequency = "A"  # Annual data
            countries_str = "+".join(imf_countries)

            # IMF API URL format
            url = f"{self.base_url}CompactData/{self.database}/{frequency}.{countries_str}.{indicator}"

            try:
                response = self.client.get(url)

                if response.status_code == 404:
                    print(f"Indicator {indicator} not found")
                    continue

                response.raise_for_status()
                data = response.json()

                # Parse the SDMX-JSON response
                series_data = self._parse_imf_response(data, indicator)
                all_data.extend(series_data)

            except httpx.HTTPStatusError as e:
                print(f"HTTP error for {indicator}: {e}")
            except Exception as e:
                print(f"Error fetching {indicator}: {e}")

        if not all_data:
            return pd.DataFrame(columns=["country", "indicator", "year", "value"])

        df = pd.DataFrame(all_data)

        # Filter by year range
        df = df[(df["year"] >= self.start_year) & (df["year"] <= self.end_year)]

        return df

    def _parse_imf_response(self, data: dict, indicator: str) -> list[dict]:
        """
        Parse IMF SDMX-JSON response.

        Args:
            data: JSON response from IMF API.
            indicator: Indicator code for reference.

        Returns:
            List of observation dictionaries.
        """
        observations = []

        try:
            # Navigate the SDMX structure
            dataset = data.get("CompactData", {}).get("DataSet", {})
            series = dataset.get("Series", [])

            # Handle single series (not a list)
            if isinstance(series, dict):
                series = [series]

            for s in series:
                # Get country code from series attributes
                country_imf = s.get("@REF_AREA", "")
                country_iso3 = IMF_TO_ISO3.get(country_imf, country_imf)

                # Get observations
                obs = s.get("Obs", [])
                if isinstance(obs, dict):
                    obs = [obs]

                for o in obs:
                    time_period = o.get("@TIME_PERIOD", "")
                    value = o.get("@OBS_VALUE")

                    if time_period and value:
                        try:
                            year = int(time_period[:4])
                            observations.append({
                                "country": country_iso3,
                                "indicator": indicator,
                                "year": year,
                                "value": float(value),
                            })
                        except (ValueError, TypeError):
                            continue

        except Exception as e:
            print(f"Error parsing IMF response: {e}")

        return observations

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
            category = session.query(Category).filter_by(code="FINANCIAL").first()

            indicator = Indicator(
                source_id=source.id,
                category_id=category.id if category else None,
                code=code,
                name=name[:255],
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
        """
        Run the full IMF data collection.

        Args:
            indicators: List of indicator codes to fetch.
            countries: List of country codes.

        Returns:
            IngestionResult with collection statistics.
        """
        result = IngestionResult(
            source=self.source_code.value,
            started_at=datetime.utcnow(),
        )

        indicator_codes = indicators or self.indicator_codes
        target_countries = countries or self.countries

        try:
            with session_scope() as session:
                source = self.get_or_create_source(session)
                log = self.create_ingestion_log(session, source)

                country_map = self._get_country_map(session)

                # Create/update indicator records
                indicator_map: dict[str, int] = {}
                for code in indicator_codes:
                    name = IMF_INDICATORS.get(code, code)
                    ind = self._get_or_create_indicator(session, source, code, name)
                    indicator_map[code] = ind.id

                # Fetch data in batches
                print(f"Fetching {len(indicator_codes)} indicators for {len(target_countries)} countries...")

                batch_size = 5
                all_data = []

                for i in range(0, len(indicator_codes), batch_size):
                    batch = indicator_codes[i:i + batch_size]
                    print(f"Fetching batch {i // batch_size + 1}: {batch}")

                    df = self.fetch_data(batch, target_countries)
                    if not df.empty:
                        all_data.append(df)

                if not all_data:
                    result.status = "completed"
                    result.completed_at = datetime.utcnow()
                    self.update_ingestion_log(session, log, result)
                    return result

                full_df = pd.concat(all_data, ignore_index=True)
                print(f"Total records fetched: {len(full_df)}")

                # Insert records
                for _, row in full_df.iterrows():
                    country_id = country_map.get(row["country"])
                    indicator_id = indicator_map.get(row["indicator"])

                    if country_id and indicator_id:
                        try:
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
                                    fetched_at=datetime.utcnow(),
                                )
                                session.add(obs)
                            result.records_processed += 1
                        except Exception as e:
                            result.records_failed += 1
                            if len(result.errors) < 10:
                                result.errors.append(str(e))

                source.last_updated = datetime.utcnow()
                result.status = "completed"
                result.completed_at = datetime.utcnow()
                self.update_ingestion_log(session, log, result)

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            result.completed_at = datetime.utcnow()

        return result


def fetch_imf_indicator(
    indicator: str,
    countries: list[str] | None = None,
    start_year: int = 1960,
    end_year: int | None = None,
) -> pd.DataFrame:
    """
    Convenience function to fetch a single IMF indicator.

    Args:
        indicator: IMF indicator code.
        countries: List of ISO3 codes.
        start_year: First year.
        end_year: Last year.

    Returns:
        DataFrame with the data.
    """
    collector = IMFCollector(
        countries=countries,
        start_year=start_year,
        end_year=end_year,
        indicators=[indicator],
    )
    return collector.fetch_data([indicator], countries)


def list_imf_databases() -> pd.DataFrame:
    """List available IMF databases."""
    return pd.DataFrame([
        {"code": k, "name": v} for k, v in IMF_DATABASES.items()
    ])


def list_imf_indicators() -> pd.DataFrame:
    """List available IMF indicators (from our curated list)."""
    return pd.DataFrame([
        {"code": k, "name": v} for k, v in IMF_INDICATORS.items()
    ])
