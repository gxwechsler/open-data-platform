"""
Data Export Utilities.

Export data to various formats: CSV, Excel, JSON, Parquet.
Supports customizable formatting and multiple export configurations.
"""

from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import Any

import pandas as pd

from open_data.config import COUNTRIES, PROJECT_ROOT, Region, get_countries_by_region
from open_data.core.query import DataQuery, QueryBuilder


class ExportFormat(str, Enum):
    """Supported export formats."""
    CSV = "csv"
    EXCEL = "xlsx"
    JSON = "json"
    PARQUET = "parquet"


class ExportConfig:
    """Configuration for data export."""

    def __init__(
        self,
        format: ExportFormat = ExportFormat.CSV,
        output_dir: Path | str | None = None,
        include_metadata: bool = True,
        timestamp_filename: bool = True,
        compression: str | None = None,
    ):
        self.format = format
        self.output_dir = Path(output_dir) if output_dir else PROJECT_ROOT / "exports"
        self.include_metadata = include_metadata
        self.timestamp_filename = timestamp_filename
        self.compression = compression

        # Create output directory if it doesn't exist
        self.output_dir.mkdir(parents=True, exist_ok=True)


class DataExporter:
    """
    Export data to various formats.
    """

    def __init__(self, config: ExportConfig | None = None):
        self.config = config or ExportConfig()

    def _generate_filename(self, base_name: str) -> Path:
        """Generate output filename."""
        if self.config.timestamp_filename:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = f"{base_name}_{timestamp}.{self.config.format.value}"
        else:
            filename = f"{base_name}.{self.config.format.value}"

        return self.config.output_dir / filename

    def _write_dataframe(
        self,
        df: pd.DataFrame,
        filepath: Path,
        sheet_name: str = "data",
    ) -> None:
        """Write DataFrame to file."""
        if self.config.format == ExportFormat.CSV:
            df.to_csv(filepath, index=False)

        elif self.config.format == ExportFormat.EXCEL:
            with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
                df.to_excel(writer, sheet_name=sheet_name, index=False)

        elif self.config.format == ExportFormat.JSON:
            df.to_json(filepath, orient="records", indent=2)

        elif self.config.format == ExportFormat.PARQUET:
            df.to_parquet(filepath, index=False)

    def export_indicator(
        self,
        indicator_code: str,
        countries: list[str] | None = None,
        start_year: int = 1960,
        end_year: int | None = None,
        filename: str | None = None,
    ) -> Path:
        """
        Export data for a single indicator.

        Args:
            indicator_code: The indicator code.
            countries: List of country codes.
            start_year: Start year.
            end_year: End year.
            filename: Custom filename (optional).

        Returns:
            Path to the exported file.
        """
        df = DataQuery.get_indicator(
            indicator_code,
            countries=countries,
            start_year=start_year,
            end_year=end_year,
        )

        base_name = filename or indicator_code.replace(".", "_")
        filepath = self._generate_filename(base_name)
        self._write_dataframe(df, filepath)

        return filepath

    def export_country(
        self,
        country_code: str,
        indicators: list[str] | None = None,
        start_year: int = 1960,
        end_year: int | None = None,
        filename: str | None = None,
    ) -> Path:
        """
        Export all data for a single country.

        Args:
            country_code: ISO3 country code.
            indicators: List of indicators (optional, defaults to all).
            start_year: Start year.
            end_year: End year.
            filename: Custom filename.

        Returns:
            Path to the exported file.
        """
        builder = (
            QueryBuilder()
            .countries(country_code)
            .years(start_year, end_year)
        )

        if indicators:
            builder.select(*indicators)

        df = builder.execute().data

        base_name = filename or f"country_{country_code}"
        filepath = self._generate_filename(base_name)
        self._write_dataframe(df, filepath)

        return filepath

    def export_region(
        self,
        region: str | Region,
        indicators: list[str] | None = None,
        start_year: int = 2000,
        end_year: int | None = None,
        filename: str | None = None,
    ) -> Path:
        """
        Export data for a region.

        Args:
            region: Region name or enum.
            indicators: List of indicators.
            start_year: Start year.
            end_year: End year.
            filename: Custom filename.

        Returns:
            Path to the exported file.
        """
        if isinstance(region, str):
            region = Region(region.upper())

        builder = (
            QueryBuilder()
            .regions(region)
            .years(start_year, end_year)
        )

        if indicators:
            builder.select(*indicators)

        df = builder.execute().data

        base_name = filename or f"region_{region.value.lower()}"
        filepath = self._generate_filename(base_name)
        self._write_dataframe(df, filepath)

        return filepath

    def export_comparison(
        self,
        countries: list[str],
        indicators: list[str],
        year: int,
        filename: str | None = None,
    ) -> Path:
        """
        Export a country comparison table.

        Args:
            countries: List of country codes.
            indicators: List of indicators.
            year: Year to compare.
            filename: Custom filename.

        Returns:
            Path to the exported file.
        """
        df = DataQuery.get_multi_indicator(indicators, countries, year)

        base_name = filename or f"comparison_{year}"
        filepath = self._generate_filename(base_name)
        self._write_dataframe(df, filepath)

        return filepath

    def export_time_series(
        self,
        indicator_code: str,
        countries: list[str],
        start_year: int = 1960,
        end_year: int | None = None,
        pivot: bool = True,
        filename: str | None = None,
    ) -> Path:
        """
        Export time series data with countries as columns.

        Args:
            indicator_code: The indicator code.
            countries: List of country codes.
            start_year: Start year.
            end_year: End year.
            pivot: If True, countries become columns.
            filename: Custom filename.

        Returns:
            Path to the exported file.
        """
        df = (
            QueryBuilder()
            .select(indicator_code)
            .countries(*countries)
            .years(start_year, end_year)
            .execute()
            .data
        )

        if pivot and not df.empty:
            df = df.pivot(
                index="year",
                columns="country",
                values="value",
            ).reset_index()

        base_name = filename or f"timeseries_{indicator_code.replace('.', '_')}"
        filepath = self._generate_filename(base_name)
        self._write_dataframe(df, filepath)

        return filepath

    def export_full_database(
        self,
        start_year: int = 1960,
        end_year: int | None = None,
        filename: str = "full_export",
    ) -> Path:
        """
        Export the entire database to a file.

        For large datasets, this uses Parquet format for efficiency.

        Args:
            start_year: Start year.
            end_year: End year.
            filename: Base filename.

        Returns:
            Path to the exported file.
        """
        df = (
            QueryBuilder()
            .years(start_year, end_year)
            .execute()
            .data
        )

        filepath = self._generate_filename(filename)
        self._write_dataframe(df, filepath)

        return filepath


def export_to_csv(
    data: pd.DataFrame,
    filename: str,
    output_dir: Path | str | None = None,
) -> Path:
    """Quick CSV export function."""
    exporter = DataExporter(ExportConfig(
        format=ExportFormat.CSV,
        output_dir=output_dir,
        timestamp_filename=False,
    ))
    filepath = (exporter.config.output_dir / filename).with_suffix(".csv")
    data.to_csv(filepath, index=False)
    return filepath


def export_to_excel(
    data: pd.DataFrame | dict[str, pd.DataFrame],
    filename: str,
    output_dir: Path | str | None = None,
) -> Path:
    """
    Quick Excel export function.

    Args:
        data: DataFrame or dict of DataFrames (for multiple sheets).
        filename: Output filename.
        output_dir: Output directory.

    Returns:
        Path to the exported file.
    """
    config = ExportConfig(
        format=ExportFormat.EXCEL,
        output_dir=output_dir,
        timestamp_filename=False,
    )
    filepath = (config.output_dir / filename).with_suffix(".xlsx")

    if isinstance(data, pd.DataFrame):
        data = {"data": data}

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        for sheet_name, df in data.items():
            df.to_excel(writer, sheet_name=sheet_name[:31], index=False)

    return filepath


def create_country_report(
    country_code: str,
    output_dir: Path | str | None = None,
) -> Path:
    """
    Create a comprehensive Excel report for a country.

    Args:
        country_code: ISO3 country code.
        output_dir: Output directory.

    Returns:
        Path to the exported file.
    """
    country = COUNTRIES.get(country_code.upper())
    if not country:
        raise ValueError(f"Unknown country code: {country_code}")

    config = ExportConfig(
        format=ExportFormat.EXCEL,
        output_dir=output_dir,
    )

    # Prepare sheets
    sheets = {}

    # Economic indicators
    economic_df = (
        QueryBuilder()
        .select(
            "NY.GDP.MKTP.CD", "NY.GDP.PCAP.CD", "NY.GDP.MKTP.KD.ZG",
            "FP.CPI.TOTL.ZG", "SL.UEM.TOTL.ZS"
        )
        .countries(country_code)
        .years(2000)
        .pivot()
        .execute()
        .data
    )
    if not economic_df.empty:
        sheets["Economic"] = economic_df

    # Trade indicators
    trade_df = (
        QueryBuilder()
        .select("NE.EXP.GNFS.ZS", "NE.IMP.GNFS.ZS", "BN.CAB.XOKA.GD.ZS")
        .countries(country_code)
        .years(2000)
        .pivot()
        .execute()
        .data
    )
    if not trade_df.empty:
        sheets["Trade"] = trade_df

    # Demographics
    demo_df = (
        QueryBuilder()
        .select("SP.POP.TOTL", "SP.POP.GROW", "SP.URB.TOTL.IN.ZS")
        .countries(country_code)
        .years(2000)
        .pivot()
        .execute()
        .data
    )
    if not demo_df.empty:
        sheets["Demographics"] = demo_df

    # Metadata sheet
    meta_df = pd.DataFrame([{
        "Country Code": country.iso3,
        "Country Name": country.name,
        "Region": country.region.value,
        "Subregion": country.subregion,
        "Report Generated": datetime.now().isoformat(),
    }])
    sheets["Metadata"] = meta_df

    # Export
    filename = f"report_{country_code}"
    filepath = config.output_dir / f"{filename}_{datetime.now().strftime('%Y%m%d')}.xlsx"

    with pd.ExcelWriter(filepath, engine="openpyxl") as writer:
        for sheet_name, df in sheets.items():
            df.to_excel(writer, sheet_name=sheet_name, index=False)

    return filepath
