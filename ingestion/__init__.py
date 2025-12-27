"""Data ingestion modules for Open Data Platform."""
from .fred_data import FREDData
from .emdat_disasters import DisasterData
from .reinhart_rogoff import CrisisData

__all__ = ['FREDData', 'DisasterData', 'CrisisData']
