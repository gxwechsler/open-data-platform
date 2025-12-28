"""
Query Engine for Open Data Platform.

Provides a high-level API for querying, filtering, and aggregating data.
Supports time series operations, cross-country comparisons, and statistical analysis.
"""

from dataclasses import dataclass
from datetime import datetime
from enum import Enum
from typing import Any, Literal

import numpy as np
import pandas as pd
from sqlalchemy import and_, func, or_
from sqlalchemy.orm import Session

from open_data.config import COUNTRIES, Region, get_countries_by_region
from open_data.db.connection import session_scope
from open_data.db.models import Category, Country, Indicator, Observation, Source


class AggregateFunction(str, Enum):
    """Supported aggregation functions."""
    SUM = "sum"
    MEAN = "mean"
    MEDIAN = "median"
    MIN = "min"
    MAX = "max"
    STD = "std"
    COUNT = "count"
    FIRST = "first"
    LAST = "last"


@dataclass
class QueryResult:
    """Result of a query operation."""
    data: pd.DataFrame
    query_time: float
    row_count: int
    metadata: dict[str, Any]

    def to_dict(self) -> dict:
        return {
            "data": self.data.to_dict("records"),
            "query_time": self.query_time,
            "row_count": self.row_count,
            "metadata": self.metadata,
        }


class QueryBuilder:
    """
    Fluent query builder for constructing complex data queries.

    Example:
        result = (
            QueryBuilder()
            .select("NY.GDP.PCAP.CD", "FP.CPI.TOTL.ZG")
            .countries("ARG", "BRA", "CHL")
            .years(2010, 2023)
            .execute()
        )
    """

    def __init__(self):
        self._indicators: list[str] = []
        self._countries: list[str] = []
        self._regions: list[Region] = []
        self._start_year: int | None = None
        self._end_year: int | None = None
        self._pivot: bool = False
        self._aggregate_by: str | None = None
        self._aggregate_func: AggregateFunction = AggregateFunction.MEAN

    def select(self, *indicators: str) -> "QueryBuilder":
        """Select indicators to query."""
        self._indicators.extend(indicators)
        return self

    def countries(self, *country_codes: str) -> "QueryBuilder":
        """Filter by country codes (ISO3)."""
        self._countries.extend(c.upper() for c in country_codes)
        return self

    def regions(self, *regions: str | Region) -> "QueryBuilder":
        """Filter by regions."""
        for r in regions:
            if isinstance(r, str):
                self._regions.append(Region(r.upper()))
            else:
                self._regions.append(r)
        return self

    def years(self, start: int, end: int | None = None) -> "QueryBuilder":
        """Filter by year range."""
        self._start_year = start
        self._end_year = end or datetime.now().year
        return self

    def year(self, year: int) -> "QueryBuilder":
        """Filter to a single year."""
        self._start_year = year
        self._end_year = year
        return self

    def pivot(self, pivot: bool = True) -> "QueryBuilder":
        """Pivot indicators as columns."""
        self._pivot = pivot
        return self

    def aggregate(
        self,
        by: Literal["country", "year", "region"],
        func: AggregateFunction | str = AggregateFunction.MEAN,
    ) -> "QueryBuilder":
        """Aggregate results."""
        self._aggregate_by = by
        if isinstance(func, str):
            self._aggregate_func = AggregateFunction(func)
        else:
            self._aggregate_func = func
        return self

    def _get_country_list(self) -> list[str]:
        """Get the final list of country codes."""
        countries = set(self._countries)

        for region in self._regions:
            region_countries = get_countries_by_region(region)
            countries.update(c.iso3 for c in region_countries)

        return list(countries) if countries else list(COUNTRIES.keys())

    def execute(self) -> QueryResult:
        """Execute the query and return results."""
        start_time = datetime.now()

        country_list = self._get_country_list()

        with session_scope() as session:
            # Build the base query
            query = (
                session.query(
                    Country.iso3_code.label("country"),
                    Country.name.label("country_name"),
                    Country.region.label("region"),
                    Indicator.code.label("indicator"),
                    Indicator.name.label("indicator_name"),
                    Observation.year,
                    Observation.value,
                )
                .join(Observation, Country.id == Observation.country_id)
                .join(Indicator, Indicator.id == Observation.indicator_id)
            )

            # Apply filters
            if self._indicators:
                query = query.filter(Indicator.code.in_(self._indicators))

            query = query.filter(Country.iso3_code.in_(country_list))

            if self._start_year:
                query = query.filter(Observation.year >= self._start_year)

            if self._end_year:
                query = query.filter(Observation.year <= self._end_year)

            # Order by
            query = query.order_by(
                Country.iso3_code,
                Indicator.code,
                Observation.year,
            )

            # Execute query
            results = query.all()

        # Convert to DataFrame
        df = pd.DataFrame(results, columns=[
            "country", "country_name", "region",
            "indicator", "indicator_name", "year", "value"
        ])

        if df.empty:
            return QueryResult(
                data=df,
                query_time=(datetime.now() - start_time).total_seconds(),
                row_count=0,
                metadata={"filters": self._get_metadata()},
            )

        # Convert value to numeric
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

        # Apply aggregation if specified
        if self._aggregate_by:
            df = self._apply_aggregation(df)

        # Pivot if requested
        if self._pivot and not self._aggregate_by:
            df = self._apply_pivot(df)

        query_time = (datetime.now() - start_time).total_seconds()

        return QueryResult(
            data=df,
            query_time=query_time,
            row_count=len(df),
            metadata={"filters": self._get_metadata()},
        )

    def _apply_aggregation(self, df: pd.DataFrame) -> pd.DataFrame:
        """Apply aggregation to the DataFrame."""
        agg_funcs = {
            AggregateFunction.SUM: "sum",
            AggregateFunction.MEAN: "mean",
            AggregateFunction.MEDIAN: "median",
            AggregateFunction.MIN: "min",
            AggregateFunction.MAX: "max",
            AggregateFunction.STD: "std",
            AggregateFunction.COUNT: "count",
            AggregateFunction.FIRST: "first",
            AggregateFunction.LAST: "last",
        }

        func_name = agg_funcs[self._aggregate_func]

        if self._aggregate_by == "country":
            return df.groupby(["country", "country_name", "indicator"]).agg({
                "value": func_name,
                "year": ["min", "max"],
            }).reset_index()

        elif self._aggregate_by == "year":
            return df.groupby(["year", "indicator"]).agg({
                "value": func_name,
                "country": "count",
            }).reset_index()

        elif self._aggregate_by == "region":
            return df.groupby(["region", "indicator", "year"]).agg({
                "value": func_name,
                "country": "count",
            }).reset_index()

        return df

    def _apply_pivot(self, df: pd.DataFrame) -> pd.DataFrame:
        """Pivot DataFrame so indicators become columns."""
        if len(self._indicators) <= 1:
            return df

        pivot = df.pivot_table(
            index=["country", "country_name", "year"],
            columns="indicator",
            values="value",
            aggfunc="first",
        ).reset_index()

        pivot.columns.name = None
        return pivot

    def _get_metadata(self) -> dict:
        """Get query metadata."""
        return {
            "indicators": self._indicators,
            "countries": self._countries,
            "regions": [r.value for r in self._regions],
            "start_year": self._start_year,
            "end_year": self._end_year,
            "pivot": self._pivot,
            "aggregate_by": self._aggregate_by,
        }


class DataQuery:
    """
    High-level data query interface.

    Provides convenient methods for common query patterns.
    """

    @staticmethod
    def get_indicator(
        indicator_code: str,
        countries: list[str] | None = None,
        start_year: int = 2000,
        end_year: int | None = None,
    ) -> pd.DataFrame:
        """
        Get data for a single indicator.

        Args:
            indicator_code: The indicator code.
            countries: List of country codes (optional).
            start_year: Start year.
            end_year: End year.

        Returns:
            DataFrame with the data.
        """
        builder = QueryBuilder().select(indicator_code).years(start_year, end_year)

        if countries:
            builder.countries(*countries)

        return builder.execute().data

    @staticmethod
    def compare_countries(
        indicator_code: str,
        countries: list[str],
        year: int | None = None,
    ) -> pd.DataFrame:
        """
        Compare countries for a specific indicator.

        Args:
            indicator_code: The indicator to compare.
            countries: List of country codes.
            year: Specific year (or latest if None).

        Returns:
            DataFrame with comparison data.
        """
        builder = (
            QueryBuilder()
            .select(indicator_code)
            .countries(*countries)
        )

        if year:
            builder.year(year)
        else:
            builder.years(2020, datetime.now().year)

        df = builder.execute().data

        if not year and not df.empty:
            # Get the latest year for each country
            df = df.loc[df.groupby("country")["year"].idxmax()]

        return df.sort_values("value", ascending=False)

    @staticmethod
    def get_time_series(
        indicator_code: str,
        country: str,
        start_year: int = 1960,
        end_year: int | None = None,
    ) -> pd.DataFrame:
        """
        Get time series for a single country and indicator.

        Args:
            indicator_code: The indicator code.
            country: Country code (ISO3).
            start_year: Start year.
            end_year: End year.

        Returns:
            DataFrame with time series.
        """
        return (
            QueryBuilder()
            .select(indicator_code)
            .countries(country)
            .years(start_year, end_year)
            .execute()
            .data
        )

    @staticmethod
    def get_latest_values(
        indicator_code: str,
        countries: list[str] | None = None,
    ) -> pd.DataFrame:
        """
        Get the most recent value for each country.

        Args:
            indicator_code: The indicator code.
            countries: List of countries (optional).

        Returns:
            DataFrame with latest values.
        """
        builder = (
            QueryBuilder()
            .select(indicator_code)
            .years(2015, datetime.now().year)
        )

        if countries:
            builder.countries(*countries)

        df = builder.execute().data

        if df.empty:
            return df

        # Get the latest year for each country
        return df.loc[df.groupby("country")["year"].idxmax()].sort_values("value", ascending=False)

    @staticmethod
    def get_regional_averages(
        indicator_code: str,
        year: int | None = None,
    ) -> pd.DataFrame:
        """
        Get average values by region.

        Args:
            indicator_code: The indicator code.
            year: Specific year (or latest if None).

        Returns:
            DataFrame with regional averages.
        """
        builder = QueryBuilder().select(indicator_code)

        if year:
            builder.year(year)
        else:
            builder.years(2020, datetime.now().year)

        builder.aggregate("region", AggregateFunction.MEAN)

        return builder.execute().data

    @staticmethod
    def get_multi_indicator(
        indicators: list[str],
        countries: list[str],
        year: int,
    ) -> pd.DataFrame:
        """
        Get multiple indicators for multiple countries in a single year.

        Args:
            indicators: List of indicator codes.
            countries: List of country codes.
            year: The year to query.

        Returns:
            DataFrame with indicators as columns.
        """
        return (
            QueryBuilder()
            .select(*indicators)
            .countries(*countries)
            .year(year)
            .pivot()
            .execute()
            .data
        )


def query(
    indicator: str | list[str],
    countries: str | list[str] | None = None,
    region: str | None = None,
    start_year: int = 2000,
    end_year: int | None = None,
) -> pd.DataFrame:
    """
    Simple query function for quick data access.

    Args:
        indicator: Indicator code(s).
        countries: Country code(s).
        region: Region name.
        start_year: Start year.
        end_year: End year.

    Returns:
        DataFrame with results.
    """
    builder = QueryBuilder()

    # Handle indicators
    if isinstance(indicator, str):
        builder.select(indicator)
    else:
        builder.select(*indicator)

    # Handle countries
    if countries:
        if isinstance(countries, str):
            builder.countries(countries)
        else:
            builder.countries(*countries)

    # Handle region
    if region:
        builder.regions(region)

    # Handle years
    builder.years(start_year, end_year)

    return builder.execute().data


def get_available_data_summary() -> pd.DataFrame:
    """
    Get a summary of available data in the database.

    Returns:
        DataFrame with indicator availability summary.
    """
    with session_scope() as session:
        summary = (
            session.query(
                Source.code.label("source"),
                Category.code.label("category"),
                Indicator.code,
                Indicator.name,
                func.count(Observation.id).label("observations"),
                func.count(func.distinct(Observation.country_id)).label("countries"),
                func.min(Observation.year).label("min_year"),
                func.max(Observation.year).label("max_year"),
            )
            .join(Indicator, Source.id == Indicator.source_id)
            .outerjoin(Category, Category.id == Indicator.category_id)
            .join(Observation, Indicator.id == Observation.indicator_id)
            .group_by(Source.code, Category.code, Indicator.code, Indicator.name)
            .order_by(Source.code, Category.code, Indicator.code)
            .all()
        )

        return pd.DataFrame(summary, columns=[
            "source", "category", "code", "name",
            "observations", "countries", "min_year", "max_year"
        ])
