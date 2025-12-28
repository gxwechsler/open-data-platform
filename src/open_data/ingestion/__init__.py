"""
Data ingestion package for Open Data Platform.
"""

from open_data.ingestion.base import BaseCollector, IngestionResult
from open_data.ingestion.imf import IMFCollector
from open_data.ingestion.world_bank import WorldBankCollector

__all__ = [
    "BaseCollector",
    "IngestionResult",
    "WorldBankCollector",
    "IMFCollector",
]
