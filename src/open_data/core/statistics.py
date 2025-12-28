"""
Statistical Analysis Module.

Provides statistical analysis capabilities:
- Descriptive statistics
- Correlation analysis
- Distribution analysis
- Comparative statistics
- Hypothesis testing
"""

from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from scipy import stats

from open_data.core.query import DataQuery, QueryBuilder


@dataclass
class DescriptiveStats:
    """Descriptive statistics for a dataset."""
    count: int
    mean: float
    std: float
    min: float
    q25: float
    median: float
    q75: float
    max: float
    skewness: float
    kurtosis: float
    cv: float  # Coefficient of variation

    def to_dict(self) -> dict[str, float]:
        return {
            "count": self.count,
            "mean": self.mean,
            "std": self.std,
            "min": self.min,
            "q25": self.q25,
            "median": self.median,
            "q75": self.q75,
            "max": self.max,
            "skewness": self.skewness,
            "kurtosis": self.kurtosis,
            "cv": self.cv,
        }


@dataclass
class CorrelationResult:
    """Result of correlation analysis."""
    indicator1: str
    indicator2: str
    correlation: float
    p_value: float
    n_observations: int
    method: str

    @property
    def is_significant(self) -> bool:
        return self.p_value < 0.05

    @property
    def strength(self) -> str:
        r = abs(self.correlation)
        if r < 0.2:
            return "negligible"
        elif r < 0.4:
            return "weak"
        elif r < 0.6:
            return "moderate"
        elif r < 0.8:
            return "strong"
        else:
            return "very strong"

    def to_dict(self) -> dict[str, Any]:
        return {
            "indicator1": self.indicator1,
            "indicator2": self.indicator2,
            "correlation": self.correlation,
            "p_value": self.p_value,
            "n_observations": self.n_observations,
            "is_significant": self.is_significant,
            "strength": self.strength,
            "method": self.method,
        }


@dataclass
class ComparisonResult:
    """Result of statistical comparison."""
    group1: str
    group2: str
    mean1: float
    mean2: float
    difference: float
    pct_difference: float
    t_statistic: float
    p_value: float
    is_significant: bool
    effect_size: float  # Cohen's d

    def to_dict(self) -> dict[str, Any]:
        return {
            "group1": self.group1,
            "group2": self.group2,
            "mean1": self.mean1,
            "mean2": self.mean2,
            "difference": self.difference,
            "pct_difference": self.pct_difference,
            "t_statistic": self.t_statistic,
            "p_value": self.p_value,
            "is_significant": self.is_significant,
            "effect_size": self.effect_size,
        }


class StatisticalAnalyzer:
    """
    Perform statistical analysis on economic data.
    """

    @staticmethod
    def descriptive_stats(values: np.ndarray | pd.Series) -> DescriptiveStats:
        """
        Calculate descriptive statistics.

        Args:
            values: Array of numeric values.

        Returns:
            DescriptiveStats object.
        """
        values = np.array(values)
        values = values[~np.isnan(values)]

        if len(values) == 0:
            raise ValueError("No valid data points")

        return DescriptiveStats(
            count=len(values),
            mean=float(np.mean(values)),
            std=float(np.std(values, ddof=1)) if len(values) > 1 else 0,
            min=float(np.min(values)),
            q25=float(np.percentile(values, 25)),
            median=float(np.median(values)),
            q75=float(np.percentile(values, 75)),
            max=float(np.max(values)),
            skewness=float(stats.skew(values)) if len(values) > 2 else 0,
            kurtosis=float(stats.kurtosis(values)) if len(values) > 3 else 0,
            cv=float(np.std(values) / np.mean(values) * 100) if np.mean(values) != 0 else 0,
        )

    @staticmethod
    def correlation(
        x: np.ndarray | pd.Series,
        y: np.ndarray | pd.Series,
        method: str = "pearson",
    ) -> CorrelationResult:
        """
        Calculate correlation between two variables.

        Args:
            x: First variable.
            y: Second variable.
            method: 'pearson', 'spearman', or 'kendall'.

        Returns:
            CorrelationResult object.
        """
        x = np.array(x)
        y = np.array(y)

        # Remove NaN pairs
        mask = ~(np.isnan(x) | np.isnan(y))
        x = x[mask]
        y = y[mask]

        if len(x) < 3:
            raise ValueError("Need at least 3 paired observations")

        if method == "pearson":
            r, p = stats.pearsonr(x, y)
        elif method == "spearman":
            r, p = stats.spearmanr(x, y)
        elif method == "kendall":
            r, p = stats.kendalltau(x, y)
        else:
            raise ValueError(f"Unknown method: {method}")

        return CorrelationResult(
            indicator1="x",
            indicator2="y",
            correlation=float(r),
            p_value=float(p),
            n_observations=len(x),
            method=method,
        )

    @staticmethod
    def correlation_matrix(
        df: pd.DataFrame,
        method: str = "pearson",
    ) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        Calculate correlation matrix.

        Args:
            df: DataFrame with numeric columns.
            method: Correlation method.

        Returns:
            Tuple of (correlation matrix, p-value matrix).
        """
        cols = df.select_dtypes(include=[np.number]).columns
        n = len(cols)

        corr_matrix = np.zeros((n, n))
        pval_matrix = np.zeros((n, n))

        for i in range(n):
            for j in range(n):
                if i == j:
                    corr_matrix[i, j] = 1.0
                    pval_matrix[i, j] = 0.0
                elif j > i:
                    try:
                        result = StatisticalAnalyzer.correlation(
                            df[cols[i]], df[cols[j]], method
                        )
                        corr_matrix[i, j] = result.correlation
                        corr_matrix[j, i] = result.correlation
                        pval_matrix[i, j] = result.p_value
                        pval_matrix[j, i] = result.p_value
                    except Exception:
                        corr_matrix[i, j] = np.nan
                        corr_matrix[j, i] = np.nan
                        pval_matrix[i, j] = np.nan
                        pval_matrix[j, i] = np.nan

        return (
            pd.DataFrame(corr_matrix, index=cols, columns=cols),
            pd.DataFrame(pval_matrix, index=cols, columns=cols),
        )

    @staticmethod
    def compare_groups(
        group1: np.ndarray | pd.Series,
        group2: np.ndarray | pd.Series,
        group1_name: str = "Group 1",
        group2_name: str = "Group 2",
    ) -> ComparisonResult:
        """
        Compare two groups statistically.

        Args:
            group1: First group values.
            group2: Second group values.
            group1_name: Name for first group.
            group2_name: Name for second group.

        Returns:
            ComparisonResult object.
        """
        g1 = np.array(group1)
        g2 = np.array(group2)

        g1 = g1[~np.isnan(g1)]
        g2 = g2[~np.isnan(g2)]

        if len(g1) < 2 or len(g2) < 2:
            raise ValueError("Need at least 2 observations per group")

        mean1 = np.mean(g1)
        mean2 = np.mean(g2)
        difference = mean2 - mean1
        pct_difference = (difference / mean1 * 100) if mean1 != 0 else 0

        # Independent samples t-test
        t_stat, p_value = stats.ttest_ind(g1, g2)

        # Cohen's d (effect size)
        pooled_std = np.sqrt(((len(g1) - 1) * np.var(g1, ddof=1) +
                             (len(g2) - 1) * np.var(g2, ddof=1)) /
                            (len(g1) + len(g2) - 2))
        effect_size = difference / pooled_std if pooled_std != 0 else 0

        return ComparisonResult(
            group1=group1_name,
            group2=group2_name,
            mean1=float(mean1),
            mean2=float(mean2),
            difference=float(difference),
            pct_difference=float(pct_difference),
            t_statistic=float(t_stat),
            p_value=float(p_value),
            is_significant=p_value < 0.05,
            effect_size=float(effect_size),
        )

    @staticmethod
    def anova(groups: dict[str, np.ndarray | pd.Series]) -> dict[str, Any]:
        """
        Perform one-way ANOVA.

        Args:
            groups: Dictionary mapping group names to values.

        Returns:
            Dictionary with F-statistic, p-value, and group means.
        """
        arrays = []
        group_names = []
        means = {}

        for name, values in groups.items():
            arr = np.array(values)
            arr = arr[~np.isnan(arr)]
            if len(arr) >= 2:
                arrays.append(arr)
                group_names.append(name)
                means[name] = float(np.mean(arr))

        if len(arrays) < 2:
            raise ValueError("Need at least 2 groups with data")

        f_stat, p_value = stats.f_oneway(*arrays)

        return {
            "f_statistic": float(f_stat),
            "p_value": float(p_value),
            "is_significant": p_value < 0.05,
            "group_means": means,
            "n_groups": len(arrays),
        }

    @staticmethod
    def percentile_rank(value: float, values: np.ndarray | pd.Series) -> float:
        """
        Calculate percentile rank of a value within a distribution.

        Args:
            value: Value to rank.
            values: Distribution values.

        Returns:
            Percentile rank (0-100).
        """
        values = np.array(values)
        values = values[~np.isnan(values)]
        return float(stats.percentileofscore(values, value))

    @staticmethod
    def z_score(value: float, values: np.ndarray | pd.Series) -> float:
        """
        Calculate z-score of a value.

        Args:
            value: Value to score.
            values: Distribution values.

        Returns:
            Z-score.
        """
        values = np.array(values)
        values = values[~np.isnan(values)]
        mean = np.mean(values)
        std = np.std(values)
        return float((value - mean) / std) if std != 0 else 0


def indicator_statistics(
    indicator: str,
    year: int | None = None,
    countries: list[str] | None = None,
) -> DescriptiveStats:
    """
    Get descriptive statistics for an indicator.

    Args:
        indicator: Indicator code.
        year: Specific year (or latest if None).
        countries: List of countries (or all if None).

    Returns:
        DescriptiveStats object.
    """
    if year:
        df = (
            QueryBuilder()
            .select(indicator)
            .year(year)
            .execute()
            .data
        )
    else:
        df = DataQuery.get_latest_values(indicator, countries)

    if df.empty:
        raise ValueError("No data found")

    return StatisticalAnalyzer.descriptive_stats(df["value"])


def correlate_indicators(
    indicator1: str,
    indicator2: str,
    year: int | None = None,
    method: str = "pearson",
) -> CorrelationResult:
    """
    Calculate correlation between two indicators across countries.

    Args:
        indicator1: First indicator code.
        indicator2: Second indicator code.
        year: Specific year (or latest if None).
        method: Correlation method.

    Returns:
        CorrelationResult object.
    """
    target_year = year or 2022

    df1 = (
        QueryBuilder()
        .select(indicator1)
        .year(target_year)
        .execute()
        .data
    )

    df2 = (
        QueryBuilder()
        .select(indicator2)
        .year(target_year)
        .execute()
        .data
    )

    if df1.empty or df2.empty:
        raise ValueError("No data found for one or both indicators")

    # Merge on country
    merged = df1.merge(
        df2,
        on="country",
        suffixes=("_1", "_2"),
    )

    if len(merged) < 3:
        raise ValueError("Not enough paired observations")

    result = StatisticalAnalyzer.correlation(
        merged["value_1"],
        merged["value_2"],
        method,
    )

    result.indicator1 = indicator1
    result.indicator2 = indicator2

    return result


def compare_regions(
    indicator: str,
    year: int | None = None,
) -> dict[str, Any]:
    """
    Compare indicator values across regions using ANOVA.

    Args:
        indicator: Indicator code.
        year: Specific year.

    Returns:
        ANOVA results with regional means.
    """
    target_year = year or 2022

    df = (
        QueryBuilder()
        .select(indicator)
        .year(target_year)
        .execute()
        .data
    )

    if df.empty:
        raise ValueError("No data found")

    # Group by region
    groups = {}
    for region in df["region"].unique():
        region_values = df[df["region"] == region]["value"].dropna().values
        if len(region_values) >= 2:
            groups[region] = region_values

    return StatisticalAnalyzer.anova(groups)


def rank_countries(
    indicator: str,
    year: int | None = None,
    ascending: bool = False,
) -> pd.DataFrame:
    """
    Rank countries by indicator value.

    Args:
        indicator: Indicator code.
        year: Specific year.
        ascending: If True, lower values rank higher.

    Returns:
        DataFrame with rankings.
    """
    df = DataQuery.get_latest_values(indicator) if year is None else (
        QueryBuilder()
        .select(indicator)
        .year(year)
        .execute()
        .data
    )

    if df.empty:
        return pd.DataFrame()

    # Calculate rankings
    df = df.sort_values("value", ascending=ascending).reset_index(drop=True)
    df["rank"] = range(1, len(df) + 1)

    # Calculate percentile
    values = df["value"].dropna().values
    df["percentile"] = df["value"].apply(
        lambda x: StatisticalAnalyzer.percentile_rank(x, values) if pd.notna(x) else None
    )

    # Calculate z-score
    df["z_score"] = df["value"].apply(
        lambda x: StatisticalAnalyzer.z_score(x, values) if pd.notna(x) else None
    )

    return df[["rank", "country", "country_name", "value", "percentile", "z_score"]]
