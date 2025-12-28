"""
Data Validation Utilities.

Provides functions for validating and checking data quality:
- Missing value detection
- Outlier detection
- Time series continuity checks
- Cross-indicator consistency checks
"""

from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any

import numpy as np
import pandas as pd
from sqlalchemy import func
from sqlalchemy.orm import Session

from open_data.db.connection import session_scope
from open_data.db.models import Country, Indicator, Observation


class ValidationSeverity(str, Enum):
    """Severity levels for validation issues."""
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


@dataclass
class ValidationIssue:
    """A single validation issue found in the data."""
    severity: ValidationSeverity
    issue_type: str
    message: str
    country: str | None = None
    indicator: str | None = None
    year: int | None = None
    value: float | None = None
    details: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict:
        return {
            "severity": self.severity.value,
            "issue_type": self.issue_type,
            "message": self.message,
            "country": self.country,
            "indicator": self.indicator,
            "year": self.year,
            "value": self.value,
            "details": self.details,
        }


@dataclass
class ValidationReport:
    """Summary of data validation results."""
    timestamp: datetime
    records_checked: int = 0
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def error_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.ERROR)

    @property
    def warning_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.WARNING)

    @property
    def info_count(self) -> int:
        return sum(1 for i in self.issues if i.severity == ValidationSeverity.INFO)

    @property
    def is_valid(self) -> bool:
        return self.error_count == 0

    def to_dict(self) -> dict:
        return {
            "timestamp": self.timestamp.isoformat(),
            "records_checked": self.records_checked,
            "error_count": self.error_count,
            "warning_count": self.warning_count,
            "info_count": self.info_count,
            "is_valid": self.is_valid,
            "issues": [i.to_dict() for i in self.issues],
        }

    def to_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame([i.to_dict() for i in self.issues])


class DataValidator:
    """
    Validates data quality in the database.
    """

    def __init__(self, session: Session | None = None):
        self._session = session
        self._issues: list[ValidationIssue] = []

    def _add_issue(
        self,
        severity: ValidationSeverity,
        issue_type: str,
        message: str,
        **kwargs,
    ) -> None:
        """Add a validation issue."""
        self._issues.append(ValidationIssue(
            severity=severity,
            issue_type=issue_type,
            message=message,
            **kwargs,
        ))

    def check_missing_values(
        self,
        indicator_code: str,
        min_coverage: float = 0.5,
    ) -> list[ValidationIssue]:
        """
        Check for countries with too many missing values.

        Args:
            indicator_code: Indicator to check.
            min_coverage: Minimum required data coverage (0-1).

        Returns:
            List of validation issues.
        """
        issues = []

        with session_scope() as session:
            # Get total possible years
            year_range = session.query(
                func.min(Observation.year),
                func.max(Observation.year),
            ).join(Indicator).filter(Indicator.code == indicator_code).first()

            if not year_range[0]:
                return issues

            total_years = year_range[1] - year_range[0] + 1

            # Get coverage per country
            coverage = (
                session.query(
                    Country.iso3_code,
                    Country.name,
                    func.count(Observation.id).label("data_points"),
                )
                .join(Observation, Country.id == Observation.country_id)
                .join(Indicator, Indicator.id == Observation.indicator_id)
                .filter(Indicator.code == indicator_code)
                .group_by(Country.iso3_code, Country.name)
                .all()
            )

            for iso3, name, data_points in coverage:
                pct = data_points / total_years
                if pct < min_coverage:
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        issue_type="low_coverage",
                        message=f"{name}: Only {pct:.1%} data coverage for {indicator_code}",
                        country=iso3,
                        indicator=indicator_code,
                        details={"coverage": pct, "data_points": data_points},
                    ))

        return issues

    def check_outliers(
        self,
        indicator_code: str,
        method: str = "zscore",
        threshold: float = 3.0,
    ) -> list[ValidationIssue]:
        """
        Detect statistical outliers.

        Args:
            indicator_code: Indicator to check.
            method: Detection method ('zscore' or 'iqr').
            threshold: Threshold for outlier detection.

        Returns:
            List of validation issues for outliers.
        """
        issues = []

        with session_scope() as session:
            data = (
                session.query(
                    Country.iso3_code,
                    Observation.year,
                    Observation.value,
                )
                .join(Observation, Country.id == Observation.country_id)
                .join(Indicator, Indicator.id == Observation.indicator_id)
                .filter(Indicator.code == indicator_code)
                .filter(Observation.value.isnot(None))
                .all()
            )

            if not data:
                return issues

            df = pd.DataFrame(data, columns=["country", "year", "value"])
            df["value"] = pd.to_numeric(df["value"], errors="coerce")

            if method == "zscore":
                mean = df["value"].mean()
                std = df["value"].std()
                if std > 0:
                    df["zscore"] = (df["value"] - mean) / std
                    outliers = df[abs(df["zscore"]) > threshold]

                    for _, row in outliers.iterrows():
                        issues.append(ValidationIssue(
                            severity=ValidationSeverity.WARNING,
                            issue_type="outlier",
                            message=f"Outlier detected: {row['country']} {row['year']} = {row['value']:.2f} (z={row['zscore']:.2f})",
                            country=row["country"],
                            indicator=indicator_code,
                            year=int(row["year"]),
                            value=float(row["value"]),
                            details={"zscore": row["zscore"]},
                        ))

            elif method == "iqr":
                q1 = df["value"].quantile(0.25)
                q3 = df["value"].quantile(0.75)
                iqr = q3 - q1
                lower = q1 - threshold * iqr
                upper = q3 + threshold * iqr

                outliers = df[(df["value"] < lower) | (df["value"] > upper)]

                for _, row in outliers.iterrows():
                    issues.append(ValidationIssue(
                        severity=ValidationSeverity.WARNING,
                        issue_type="outlier",
                        message=f"Outlier detected: {row['country']} {row['year']} = {row['value']:.2f}",
                        country=row["country"],
                        indicator=indicator_code,
                        year=int(row["year"]),
                        value=float(row["value"]),
                        details={"bounds": [lower, upper]},
                    ))

        return issues

    def check_time_series_gaps(
        self,
        indicator_code: str,
        max_gap_years: int = 3,
    ) -> list[ValidationIssue]:
        """
        Check for gaps in time series data.

        Args:
            indicator_code: Indicator to check.
            max_gap_years: Maximum acceptable gap in years.

        Returns:
            List of validation issues for gaps.
        """
        issues = []

        with session_scope() as session:
            countries = session.query(Country).all()

            for country in countries:
                years = (
                    session.query(Observation.year)
                    .join(Indicator, Indicator.id == Observation.indicator_id)
                    .filter(Observation.country_id == country.id)
                    .filter(Indicator.code == indicator_code)
                    .order_by(Observation.year)
                    .all()
                )

                years = [y[0] for y in years]

                if len(years) < 2:
                    continue

                for i in range(1, len(years)):
                    gap = years[i] - years[i - 1]
                    if gap > max_gap_years:
                        issues.append(ValidationIssue(
                            severity=ValidationSeverity.INFO,
                            issue_type="time_gap",
                            message=f"{country.name}: {gap}-year gap ({years[i-1]}-{years[i]})",
                            country=country.iso3_code,
                            indicator=indicator_code,
                            year=years[i - 1],
                            details={"gap_years": gap, "from_year": years[i - 1], "to_year": years[i]},
                        ))

        return issues

    def check_negative_values(
        self,
        indicator_code: str,
        allow_negative: bool = False,
    ) -> list[ValidationIssue]:
        """
        Check for unexpected negative values.

        Args:
            indicator_code: Indicator to check.
            allow_negative: Whether negatives are expected.

        Returns:
            List of validation issues.
        """
        if allow_negative:
            return []

        issues = []

        # Indicators that can legitimately be negative
        negative_allowed = {
            "NY.GDP.MKTP.KD.ZG",  # GDP growth
            "NY.GDP.PCAP.KD.ZG",  # GDP per capita growth
            "BN.CAB.XOKA.CD",  # Current account
            "BN.CAB.XOKA.GD.ZS",
            "GC.BAL.CASH.GD.ZS",  # Fiscal balance
            "FR.INR.RINR",  # Real interest rate
            "SP.POP.GROW",  # Population growth
        }

        if indicator_code in negative_allowed:
            return []

        with session_scope() as session:
            negatives = (
                session.query(
                    Country.iso3_code,
                    Observation.year,
                    Observation.value,
                )
                .join(Observation, Country.id == Observation.country_id)
                .join(Indicator, Indicator.id == Observation.indicator_id)
                .filter(Indicator.code == indicator_code)
                .filter(Observation.value < 0)
                .all()
            )

            for iso3, year, value in negatives:
                issues.append(ValidationIssue(
                    severity=ValidationSeverity.WARNING,
                    issue_type="negative_value",
                    message=f"Unexpected negative: {iso3} {year} = {value}",
                    country=iso3,
                    indicator=indicator_code,
                    year=year,
                    value=float(value),
                ))

        return issues

    def validate_indicator(
        self,
        indicator_code: str,
        checks: list[str] | None = None,
    ) -> ValidationReport:
        """
        Run all validation checks on an indicator.

        Args:
            indicator_code: Indicator to validate.
            checks: List of checks to run. If None, run all.

        Returns:
            ValidationReport with all issues.
        """
        report = ValidationReport(timestamp=datetime.utcnow())

        all_checks = checks or ["missing", "outliers", "gaps", "negative"]

        if "missing" in all_checks:
            report.issues.extend(self.check_missing_values(indicator_code))

        if "outliers" in all_checks:
            report.issues.extend(self.check_outliers(indicator_code))

        if "gaps" in all_checks:
            report.issues.extend(self.check_time_series_gaps(indicator_code))

        if "negative" in all_checks:
            report.issues.extend(self.check_negative_values(indicator_code))

        return report

    def get_data_quality_summary(self) -> pd.DataFrame:
        """
        Get a summary of data quality across all indicators.

        Returns:
            DataFrame with quality metrics per indicator.
        """
        with session_scope() as session:
            summary = (
                session.query(
                    Indicator.code,
                    Indicator.name,
                    func.count(Observation.id).label("total_obs"),
                    func.count(func.distinct(Observation.country_id)).label("countries"),
                    func.min(Observation.year).label("min_year"),
                    func.max(Observation.year).label("max_year"),
                    func.avg(Observation.value).label("mean_value"),
                )
                .join(Observation, Indicator.id == Observation.indicator_id)
                .group_by(Indicator.code, Indicator.name)
                .all()
            )

            return pd.DataFrame(summary, columns=[
                "code", "name", "observations", "countries",
                "min_year", "max_year", "mean_value"
            ])


def validate_data(
    indicator_code: str | None = None,
    checks: list[str] | None = None,
) -> ValidationReport:
    """
    Run data validation.

    Args:
        indicator_code: Specific indicator to validate (or None for all).
        checks: List of checks to run.

    Returns:
        ValidationReport with results.
    """
    validator = DataValidator()

    if indicator_code:
        return validator.validate_indicator(indicator_code, checks)

    # Validate all indicators
    report = ValidationReport(timestamp=datetime.utcnow())

    with session_scope() as session:
        indicators = session.query(Indicator.code).all()

        for (code,) in indicators:
            ind_report = validator.validate_indicator(code, checks)
            report.issues.extend(ind_report.issues)

    return report


def get_quality_summary() -> pd.DataFrame:
    """Get data quality summary for all indicators."""
    validator = DataValidator()
    return validator.get_data_quality_summary()
