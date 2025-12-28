"""
Core analysis module for Open Data Platform.

Provides:
- Query engine for data access
- Indicator catalog
- Data validation
- Export utilities
- Time series analysis
- Statistical analysis
- Country clustering
"""

from open_data.core.catalog import (
    IndicatorCatalog,
    catalog,
    get_indicator_info,
    list_categories,
    search_indicators,
)
from open_data.core.clustering import (
    ClusterProfile,
    ClusterResult,
    CountryClusterer,
    PCAResult,
    cluster_countries,
    find_similar_countries,
    segment_by_development,
    segment_by_economy,
)
from open_data.core.export import (
    DataExporter,
    ExportConfig,
    ExportFormat,
    create_country_report,
    export_to_csv,
    export_to_excel,
)
from open_data.core.query import (
    DataQuery,
    QueryBuilder,
    QueryResult,
    get_available_data_summary,
    query,
)
from open_data.core.statistics import (
    ComparisonResult,
    CorrelationResult,
    DescriptiveStats,
    StatisticalAnalyzer,
    compare_regions,
    correlate_indicators,
    indicator_statistics,
    rank_countries,
)
from open_data.core.timeseries import (
    ForecastMethod,
    ForecastResult,
    TimeSeriesAnalyzer,
    TrendAnalysis,
    TrendType,
    analyze_indicator_trend,
    compare_trends,
    forecast_indicator,
)
from open_data.core.validation import (
    DataValidator,
    ValidationReport,
    get_quality_summary,
    validate_data,
)

__all__ = [
    # Query
    "QueryBuilder",
    "QueryResult",
    "DataQuery",
    "query",
    "get_available_data_summary",
    # Catalog
    "IndicatorCatalog",
    "catalog",
    "search_indicators",
    "get_indicator_info",
    "list_categories",
    # Export
    "DataExporter",
    "ExportConfig",
    "ExportFormat",
    "export_to_csv",
    "export_to_excel",
    "create_country_report",
    # Validation
    "DataValidator",
    "ValidationReport",
    "validate_data",
    "get_quality_summary",
    # Time Series
    "TimeSeriesAnalyzer",
    "TrendAnalysis",
    "TrendType",
    "ForecastResult",
    "ForecastMethod",
    "analyze_indicator_trend",
    "forecast_indicator",
    "compare_trends",
    # Statistics
    "StatisticalAnalyzer",
    "DescriptiveStats",
    "CorrelationResult",
    "ComparisonResult",
    "indicator_statistics",
    "correlate_indicators",
    "compare_regions",
    "rank_countries",
    # Clustering
    "CountryClusterer",
    "ClusterResult",
    "ClusterProfile",
    "PCAResult",
    "cluster_countries",
    "segment_by_development",
    "segment_by_economy",
    "find_similar_countries",
]
