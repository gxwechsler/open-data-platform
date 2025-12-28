"""
Base classes for data ingestion.
"""

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

from sqlalchemy.orm import Session

from open_data.config import COUNTRY_CODES, DataSource
from open_data.db.connection import session_scope
from open_data.db.models import IngestionLog, Source


@dataclass
class IngestionResult:
    """Result of a data ingestion operation."""

    source: str
    started_at: datetime
    completed_at: datetime | None = None
    status: str = "running"
    records_processed: int = 0
    records_failed: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def duration_seconds(self) -> float | None:
        """Calculate duration in seconds."""
        if self.completed_at:
            return (self.completed_at - self.started_at).total_seconds()
        return None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary."""
        return {
            "source": self.source,
            "started_at": self.started_at.isoformat(),
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "status": self.status,
            "records_processed": self.records_processed,
            "records_failed": self.records_failed,
            "duration_seconds": self.duration_seconds,
            "errors": self.errors,
        }


class BaseCollector(ABC):
    """
    Abstract base class for data collectors.

    All data source collectors should inherit from this class
    and implement the required methods.
    """

    source_code: DataSource
    source_name: str
    base_url: str

    def __init__(
        self,
        countries: list[str] | None = None,
        start_year: int = 1960,
        end_year: int | None = None,
    ):
        """
        Initialize the collector.

        Args:
            countries: List of ISO3 country codes. If None, use all configured countries.
            start_year: First year to collect data for.
            end_year: Last year to collect data for. If None, use current year.
        """
        self.countries = countries or COUNTRY_CODES
        self.start_year = start_year
        self.end_year = end_year or datetime.now().year
        self._validate_countries()

    def _validate_countries(self) -> None:
        """Validate that all country codes are valid."""
        invalid = set(self.countries) - set(COUNTRY_CODES)
        if invalid:
            raise ValueError(f"Invalid country codes: {invalid}")

    def get_or_create_source(self, session: Session) -> Source:
        """
        Get or create the source record in the database.

        Args:
            session: SQLAlchemy session.

        Returns:
            Source model instance.
        """
        source = session.query(Source).filter_by(code=self.source_code.value).first()
        if not source:
            source = Source(
                code=self.source_code.value,
                name=self.source_name,
                base_url=self.base_url,
            )
            session.add(source)
            session.flush()
        return source

    def create_ingestion_log(self, session: Session, source: Source) -> IngestionLog:
        """
        Create an ingestion log entry.

        Args:
            session: SQLAlchemy session.
            source: Source model instance.

        Returns:
            IngestionLog model instance.
        """
        log = IngestionLog(
            source_id=source.id,
            started_at=datetime.utcnow(),
            status="running",
        )
        session.add(log)
        session.flush()
        return log

    def update_ingestion_log(
        self,
        session: Session,
        log: IngestionLog,
        result: IngestionResult,
    ) -> None:
        """
        Update the ingestion log with results.

        Args:
            session: SQLAlchemy session.
            log: IngestionLog model instance.
            result: IngestionResult with operation details.
        """
        log.completed_at = result.completed_at
        log.status = result.status
        log.records_processed = result.records_processed
        if result.errors:
            log.error_message = "\n".join(result.errors[:10])  # Keep first 10 errors
        session.flush()

    @abstractmethod
    def fetch_indicators(self) -> list[dict[str, Any]]:
        """
        Fetch available indicators from the data source.

        Returns:
            List of indicator definitions.
        """
        pass

    @abstractmethod
    def fetch_data(
        self,
        indicators: list[str],
        countries: list[str] | None = None,
    ) -> list[dict[str, Any]]:
        """
        Fetch data for specified indicators and countries.

        Args:
            indicators: List of indicator codes to fetch.
            countries: List of country codes. If None, use all configured.

        Returns:
            List of observation records.
        """
        pass

    @abstractmethod
    def collect(
        self,
        indicators: list[str] | None = None,
        countries: list[str] | None = None,
    ) -> IngestionResult:
        """
        Run the full collection process.

        This method should:
        1. Fetch indicators if not specified
        2. Fetch data for all indicators and countries
        3. Store data in the database
        4. Return an IngestionResult with status

        Args:
            indicators: Optional list of indicator codes. If None, fetch all.
            countries: Optional list of country codes. If None, use all configured.

        Returns:
            IngestionResult with operation status and statistics.
        """
        pass

    def run(
        self,
        indicators: list[str] | None = None,
        countries: list[str] | None = None,
    ) -> IngestionResult:
        """
        Execute the collection with database transaction management.

        Args:
            indicators: Optional list of indicator codes.
            countries: Optional list of country codes.

        Returns:
            IngestionResult with operation status.
        """
        result = IngestionResult(
            source=self.source_code.value,
            started_at=datetime.utcnow(),
        )

        try:
            with session_scope() as session:
                source = self.get_or_create_source(session)
                log = self.create_ingestion_log(session, source)

                result = self.collect(indicators, countries)

                self.update_ingestion_log(session, log, result)

        except Exception as e:
            result.status = "failed"
            result.errors.append(str(e))
            result.completed_at = datetime.utcnow()

        return result
