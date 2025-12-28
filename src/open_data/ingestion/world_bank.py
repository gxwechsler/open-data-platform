"""
World Bank Open Data API client.

Uses the official wbgapi library for data retrieval.
Documentation: https://github.com/tgherzog/wbgapi
API: https://datahelpdesk.worldbank.org/knowledgebase/topics/125589
"""

from datetime import datetime
from decimal import Decimal
from typing import Any

import pandas as pd
import wbgapi as wb
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.orm import Session

from open_data.config import (
    COUNTRY_CODES,
    WORLD_BANK_INDICATORS,
    DataSource,
    settings,
)
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source
from open_data.ingestion.base import BaseCollector, IngestionResult


class WorldBankCollector(BaseCollector):
    """Collector for World Bank Open Data."""

    source_code = DataSource.WORLD_BANK
    source_name = "World Bank"
    base_url = "https://api.worldbank.org/v2/"

    def __init__(
        self,
        countries: list[str] | None = None,
        start_year: int = 1960,
        end_year: int | None = None,
        indicators: list[str] | None = None,
    ):
        """
        Initialize World Bank collector.

        Args:
            countries: List of ISO3 country codes.
            start_year: First year to collect.
            end_year: Last year to collect.
            indicators: List of indicator codes. If None, use default economic indicators.
        """
        super().__init__(countries, start_year, end_year)
        self.indicator_codes = indicators or list(WORLD_BANK_INDICATORS.keys())

    def fetch_indicators(self) -> list[dict[str, Any]]:
        """
        Fetch indicator metadata from World Bank API.

        Returns:
            List of indicator definitions with code, name, description, etc.
        """
        indicators = []
        for code in self.indicator_codes:
            try:
                info = wb.series.get(code)
                indicators.append(
                    {
                        "code": code,
                        "name": info.get("value", WORLD_BANK_INDICATORS.get(code, code)),
                        "description": info.get("sourceNote", ""),
                        "unit": info.get("unit", ""),
                        "source": "WB",
                    }
                )
            except Exception:
                # Fallback to our predefined names
                indicators.append(
                    {
                        "code": code,
                        "name": WORLD_BANK_INDICATORS.get(code, code),
                        "description": "",
                        "unit": "",
                        "source": "WB",
                    }
                )
        return indicators

    def fetch_data(
        self,
        indicators: list[str],
        countries: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Fetch data for specified indicators and countries.

        Args:
            indicators: List of World Bank indicator codes.
            countries: List of ISO3 country codes.

        Returns:
            DataFrame with columns: country, indicator, year, value
        """
        target_countries = countries or self.countries
        time_range = range(self.start_year, self.end_year + 1)

        try:
            # Fetch data using wbgapi
            # The library handles pagination automatically
            df = wb.data.DataFrame(
                indicators,
                economy=target_countries,
                time=time_range,
                labels=False,  # Use codes instead of labels
                columns="series",  # Indicators as columns
                numericTimeKeys=True,  # Years as integers
            )

            if df.empty:
                return pd.DataFrame(columns=["country", "indicator", "year", "value"])

            # Reset index to get country and year as columns
            df = df.reset_index()

            # Melt to long format
            df = df.melt(
                id_vars=["economy", "time"],
                var_name="indicator",
                value_name="value",
            )

            # Rename columns
            df = df.rename(columns={"economy": "country", "time": "year"})

            # Remove null values
            df = df.dropna(subset=["value"])

            return df

        except Exception as e:
            print(f"Error fetching data: {e}")
            return pd.DataFrame(columns=["country", "indicator", "year", "value"])

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
            # Try to find appropriate category
            category = session.query(Category).filter_by(code="ECONOMIC").first()

            indicator = Indicator(
                source_id=source.id,
                category_id=category.id if category else None,
                code=code,
                name=name[:255],  # Truncate if too long
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
        Run the full World Bank data collection.

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
                # Get source
                source = self.get_or_create_source(session)
                log = self.create_ingestion_log(session, source)

                # Get country ID mapping
                country_map = self._get_country_map(session)

                # Fetch indicator metadata and create/update records
                indicator_map: dict[str, int] = {}
                for code in indicator_codes:
                    name = WORLD_BANK_INDICATORS.get(code, code)
                    ind = self._get_or_create_indicator(session, source, code, name)
                    indicator_map[code] = ind.id

                # Fetch data in batches (to avoid API limits)
                batch_size = 10
                all_data = []

                for i in range(0, len(indicator_codes), batch_size):
                    batch = indicator_codes[i : i + batch_size]
                    print(f"Fetching batch {i // batch_size + 1}: {batch}")

                    df = self.fetch_data(batch, target_countries)
                    if not df.empty:
                        all_data.append(df)

                if not all_data:
                    result.status = "completed"
                    result.completed_at = datetime.utcnow()
                    return result

                # Combine all data
                full_df = pd.concat(all_data, ignore_index=True)
                print(f"Total records fetched: {len(full_df)}")

                # Prepare records for bulk insert
                records = []
                for _, row in full_df.iterrows():
                    country_id = country_map.get(row["country"])
                    indicator_id = indicator_map.get(row["indicator"])

                    if country_id and indicator_id:
                        records.append(
                            {
                                "country_id": country_id,
                                "indicator_id": indicator_id,
                                "year": int(row["year"]),
                                "value": Decimal(str(row["value"])) if pd.notna(row["value"]) else None,
                                "is_estimated": False,
                                "fetched_at": datetime.utcnow(),
                            }
                        )

                # Bulk upsert using PostgreSQL ON CONFLICT
                if records:
                    # Process in chunks to avoid memory issues
                    chunk_size = 5000
                    for i in range(0, len(records), chunk_size):
                        chunk = records[i : i + chunk_size]

                        stmt = insert(Observation).values(chunk)
                        stmt = stmt.on_conflict_do_update(
                            index_elements=["id", "year"],
                            set_={
                                "value": stmt.excluded.value,
                                "fetched_at": stmt.excluded.fetched_at,
                            },
                        )
                        # For partitioned tables, we use simpler insert
                        # and let duplicates fail silently or use a different approach
                        try:
                            session.execute(insert(Observation).values(chunk))
                        except Exception:
                            # Insert one by one for conflict handling
                            for record in chunk:
                                try:
                                    existing = (
                                        session.query(Observation)
                                        .filter_by(
                                            country_id=record["country_id"],
                                            indicator_id=record["indicator_id"],
                                            year=record["year"],
                                        )
                                        .first()
                                    )
                                    if existing:
                                        existing.value = record["value"]
                                        existing.fetched_at = record["fetched_at"]
                                    else:
                                        session.add(Observation(**record))
                                except Exception as e:
                                    result.records_failed += 1
                                    if len(result.errors) < 10:
                                        result.errors.append(str(e))

                        result.records_processed += len(chunk)

                # Update source last_updated
                source.last_updated = datetime.utcnow()

                result.status = "completed"
                result.completed_at = datetime.utcnow()

                self.update_ingestion_log(session, log, result)

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            result.completed_at = datetime.utcnow()

        return result


def fetch_single_indicator(
    indicator: str,
    countries: list[str] | None = None,
    start_year: int = 1960,
    end_year: int | None = None,
) -> pd.DataFrame:
    """
    Convenience function to fetch a single indicator.

    Args:
        indicator: World Bank indicator code.
        countries: List of ISO3 codes. Defaults to all configured countries.
        start_year: First year.
        end_year: Last year.

    Returns:
        DataFrame with the data.
    """
    collector = WorldBankCollector(
        countries=countries,
        start_year=start_year,
        end_year=end_year,
        indicators=[indicator],
    )
    return collector.fetch_data([indicator], countries)


def list_available_indicators(search: str | None = None) -> pd.DataFrame:
    """
    List available World Bank indicators.

    Args:
        search: Optional search term to filter indicators.

    Returns:
        DataFrame with indicator codes and names.
    """
    if search:
        results = wb.series.info(q=search)
    else:
        results = wb.series.info()

    data = []
    for item in results:
        data.append(
            {
                "code": item["id"],
                "name": item["value"],
            }
        )

    return pd.DataFrame(data)
