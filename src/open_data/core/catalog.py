"""
Indicator Catalog - Central registry for all indicators across data sources.

Provides a unified interface for discovering, searching, and managing indicators
from World Bank, IMF, and other data sources.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any

import pandas as pd
from sqlalchemy.orm import Session

from open_data.config import (
    WORLD_BANK_INDICATORS,
    Category as CategoryEnum,
    DataSource,
)
from open_data.db.connection import session_scope
from open_data.db.models import Category, Indicator, Source
from open_data.ingestion.imf import IMF_INDICATORS


class IndicatorFrequency(str, Enum):
    """Data frequency types."""
    ANNUAL = "annual"
    QUARTERLY = "quarterly"
    MONTHLY = "monthly"
    DAILY = "daily"


@dataclass
class IndicatorInfo:
    """Detailed indicator information."""
    code: str
    name: str
    source: str
    category: str
    description: str = ""
    unit: str = ""
    frequency: str = "annual"
    start_year: int | None = None
    end_year: int | None = None
    country_coverage: int = 0

    def to_dict(self) -> dict[str, Any]:
        return {
            "code": self.code,
            "name": self.name,
            "source": self.source,
            "category": self.category,
            "description": self.description,
            "unit": self.unit,
            "frequency": self.frequency,
            "start_year": self.start_year,
            "end_year": self.end_year,
            "country_coverage": self.country_coverage,
        }


# Master indicator catalog with categorization
INDICATOR_CATALOG = {
    # ==========================================================================
    # ECONOMIC INDICATORS
    # ==========================================================================
    "economic": {
        "gdp": {
            "NY.GDP.MKTP.CD": ("GDP (current US$)", "WB", "Current GDP in US dollars"),
            "NY.GDP.MKTP.KD": ("GDP (constant 2015 US$)", "WB", "Real GDP at 2015 prices"),
            "NY.GDP.MKTP.KD.ZG": ("GDP growth (annual %)", "WB", "Annual GDP growth rate"),
            "NY.GDP.PCAP.CD": ("GDP per capita (current US$)", "WB", "GDP divided by population"),
            "NY.GDP.PCAP.KD": ("GDP per capita (constant 2015 US$)", "WB", "Real GDP per capita"),
            "NY.GDP.PCAP.KD.ZG": ("GDP per capita growth (annual %)", "WB", "GDP per capita growth"),
            "NY.GDP.PCAP.PP.CD": ("GDP per capita, PPP", "WB", "GDP per capita at PPP"),
            "NGDP_XDC": ("GDP, Current Prices (National Currency)", "IMF", "Nominal GDP in local currency"),
            "NGDP_R_XDC": ("GDP, Constant Prices (National Currency)", "IMF", "Real GDP in local currency"),
        },
        "trade": {
            "NE.EXP.GNFS.ZS": ("Exports (% of GDP)", "WB", "Export share of GDP"),
            "NE.IMP.GNFS.ZS": ("Imports (% of GDP)", "WB", "Import share of GDP"),
            "NE.TRD.GNFS.ZS": ("Trade (% of GDP)", "WB", "Total trade as share of GDP"),
            "BN.CAB.XOKA.CD": ("Current Account Balance (USD)", "WB", "Current account in USD"),
            "BN.CAB.XOKA.GD.ZS": ("Current Account (% of GDP)", "WB", "Current account share"),
            "BCA_BP6_USD": ("Current Account Balance (USD)", "IMF", "IMF current account"),
            "BXG_BP6_USD": ("Exports of Goods (USD)", "IMF", "Goods exports"),
            "BMG_BP6_USD": ("Imports of Goods (USD)", "IMF", "Goods imports"),
        },
        "inflation": {
            "FP.CPI.TOTL.ZG": ("Inflation, CPI (annual %)", "WB", "Consumer price inflation"),
            "FP.CPI.TOTL": ("Consumer Price Index (2010=100)", "WB", "CPI index"),
            "PCPI_IX": ("Consumer Price Index (2010=100)", "IMF", "IMF CPI index"),
            "PCPI_PC_CP_A_PT": ("Inflation Rate (%)", "IMF", "IMF inflation rate"),
            "PPPI_IX": ("Producer Price Index (2010=100)", "IMF", "PPI index"),
        },
        "employment": {
            "SL.UEM.TOTL.ZS": ("Unemployment (% of labor force)", "WB", "Unemployment rate"),
            "SL.UEM.TOTL.NE.ZS": ("Unemployment, national estimate (%)", "WB", "National unemployment"),
            "SL.TLF.TOTL.IN": ("Labor force, total", "WB", "Total labor force"),
            "SL.TLF.ACTI.ZS": ("Labor force participation rate (%)", "WB", "Participation rate"),
        },
    },
    # ==========================================================================
    # FINANCIAL INDICATORS
    # ==========================================================================
    "financial": {
        "interest_rates": {
            "FR.INR.RINR": ("Real interest rate (%)", "WB", "Inflation-adjusted rate"),
            "FR.INR.LEND": ("Lending interest rate (%)", "WB", "Bank lending rate"),
            "FPOLM_PA": ("Monetary Policy Rate (%)", "IMF", "Central bank policy rate"),
            "FITB_PA": ("Treasury Bill Rate (%)", "IMF", "T-bill rate"),
            "FILR_PA": ("Lending Rate (%)", "IMF", "IMF lending rate"),
            "FIDR_PA": ("Deposit Rate (%)", "IMF", "Bank deposit rate"),
        },
        "exchange_rates": {
            "PA.NUS.FCRF": ("Exchange Rate (LCU per USD)", "WB", "Official exchange rate"),
            "ENDA_XDC_USD_RATE": ("Exchange Rate, End of Period", "IMF", "End of period rate"),
            "ENEA_XDC_USD_RATE": ("Exchange Rate, Period Average", "IMF", "Average rate"),
        },
        "money": {
            "FM.LBL.BMNY.GD.ZS": ("Broad money (% of GDP)", "WB", "M2 as share of GDP"),
            "FM_A": ("Broad Money (National Currency)", "IMF", "M2 in local currency"),
            "FMB_XDC": ("Monetary Base (National Currency)", "IMF", "M0 in local currency"),
        },
        "investment": {
            "BX.KLT.DINV.CD.WD": ("FDI, net inflows (USD)", "WB", "Foreign direct investment"),
            "BX.KLT.DINV.WD.GD.ZS": ("FDI, net inflows (% of GDP)", "WB", "FDI share of GDP"),
            "NE.GDI.TOTL.ZS": ("Gross capital formation (% of GDP)", "WB", "Investment rate"),
        },
        "debt": {
            "DT.DOD.DECT.CD": ("External debt stocks (USD)", "WB", "Total external debt"),
            "DT.DOD.DECT.GN.ZS": ("External debt (% of GNI)", "WB", "Debt to income ratio"),
            "GC.DOD.TOTL.GD.ZS": ("Central govt debt (% of GDP)", "WB", "Government debt"),
        },
        "reserves": {
            "RAFA_USD": ("Total Reserves (USD)", "IMF", "International reserves"),
            "RAFAGOLD_USD": ("Gold Reserves (USD)", "IMF", "Gold holdings"),
            "FI.RES.TOTL.CD": ("Total reserves (includes gold, USD)", "WB", "WB total reserves"),
        },
    },
    # ==========================================================================
    # DEMOGRAPHIC INDICATORS
    # ==========================================================================
    "demographic": {
        "population": {
            "SP.POP.TOTL": ("Population, total", "WB", "Total population"),
            "SP.POP.GROW": ("Population growth (annual %)", "WB", "Population growth rate"),
            "EN.POP.DNST": ("Population density (per sq km)", "WB", "People per square km"),
            "SP.URB.TOTL.IN.ZS": ("Urban population (% of total)", "WB", "Urbanization rate"),
        },
        "age_structure": {
            "SP.POP.0014.TO.ZS": ("Population ages 0-14 (%)", "WB", "Youth share"),
            "SP.POP.1564.TO.ZS": ("Population ages 15-64 (%)", "WB", "Working age share"),
            "SP.POP.65UP.TO.ZS": ("Population ages 65+ (%)", "WB", "Elderly share"),
            "SP.POP.DPND": ("Age dependency ratio (%)", "WB", "Dependents per worker"),
        },
        "fertility": {
            "SP.DYN.TFRT.IN": ("Fertility rate (births per woman)", "WB", "Total fertility"),
            "SP.DYN.CBRT.IN": ("Birth rate (per 1,000)", "WB", "Crude birth rate"),
            "SP.DYN.CDRT.IN": ("Death rate (per 1,000)", "WB", "Crude death rate"),
        },
    },
    # ==========================================================================
    # HEALTH INDICATORS
    # ==========================================================================
    "health": {
        "mortality": {
            "SP.DYN.LE00.IN": ("Life expectancy at birth", "WB", "Average life expectancy"),
            "SP.DYN.IMRT.IN": ("Infant mortality (per 1,000)", "WB", "Deaths under 1 year"),
            "SH.DYN.MORT": ("Under-5 mortality (per 1,000)", "WB", "Deaths under 5 years"),
            "SH.STA.MMRT": ("Maternal mortality ratio", "WB", "Deaths per 100,000 births"),
        },
        "healthcare": {
            "SH.XPD.CHEX.GD.ZS": ("Health expenditure (% of GDP)", "WB", "Health spending"),
            "SH.MED.BEDS.ZS": ("Hospital beds (per 1,000)", "WB", "Hospital capacity"),
            "SH.MED.PHYS.ZS": ("Physicians (per 1,000)", "WB", "Doctor density"),
        },
        "disease": {
            "SH.TBS.INCD": ("TB incidence (per 100,000)", "WB", "Tuberculosis rate"),
            "SH.HIV.INCD.ZS": ("HIV incidence (per 1,000)", "WB", "New HIV infections"),
        },
    },
    # ==========================================================================
    # EDUCATION INDICATORS
    # ==========================================================================
    "education": {
        "enrollment": {
            "SE.PRM.ENRR": ("Primary enrollment rate (%)", "WB", "Primary school enrollment"),
            "SE.SEC.ENRR": ("Secondary enrollment rate (%)", "WB", "Secondary enrollment"),
            "SE.TER.ENRR": ("Tertiary enrollment rate (%)", "WB", "University enrollment"),
        },
        "literacy": {
            "SE.ADT.LITR.ZS": ("Adult literacy rate (%)", "WB", "Adults who can read"),
            "SE.ADT.1524.LT.ZS": ("Youth literacy rate (%)", "WB", "Youth who can read"),
        },
        "spending": {
            "SE.XPD.TOTL.GD.ZS": ("Education expenditure (% of GDP)", "WB", "Education spending"),
            "SE.XPD.TOTL.GB.ZS": ("Education (% of govt expenditure)", "WB", "Govt education budget"),
        },
    },
    # ==========================================================================
    # GOVERNMENT INDICATORS
    # ==========================================================================
    "government": {
        "fiscal": {
            "GC.REV.XGRT.GD.ZS": ("Revenue (% of GDP)", "WB", "Government revenue"),
            "GC.XPN.TOTL.GD.ZS": ("Expense (% of GDP)", "WB", "Government spending"),
            "GC.BAL.CASH.GD.ZS": ("Cash surplus/deficit (% of GDP)", "WB", "Fiscal balance"),
            "GC.TAX.TOTL.GD.ZS": ("Tax revenue (% of GDP)", "WB", "Tax collection"),
        },
    },
}


class IndicatorCatalog:
    """
    Central catalog for discovering and managing indicators.
    """

    def __init__(self):
        self._indicators: dict[str, IndicatorInfo] = {}
        self._load_catalog()

    def _load_catalog(self) -> None:
        """Load all indicators from the catalog."""
        for category, subcategories in INDICATOR_CATALOG.items():
            for subcategory, indicators in subcategories.items():
                for code, (name, source, description) in indicators.items():
                    self._indicators[code] = IndicatorInfo(
                        code=code,
                        name=name,
                        source=source,
                        category=category,
                        description=description,
                        frequency="annual",
                    )

    def search(
        self,
        query: str | None = None,
        category: str | None = None,
        source: str | None = None,
    ) -> list[IndicatorInfo]:
        """
        Search for indicators.

        Args:
            query: Text to search in name/description.
            category: Filter by category (economic, financial, etc.)
            source: Filter by source (WB, IMF)

        Returns:
            List of matching IndicatorInfo objects.
        """
        results = list(self._indicators.values())

        if query:
            query_lower = query.lower()
            results = [
                i for i in results
                if query_lower in i.name.lower() or query_lower in i.description.lower()
            ]

        if category:
            results = [i for i in results if i.category == category.lower()]

        if source:
            results = [i for i in results if i.source == source.upper()]

        return results

    def get(self, code: str) -> IndicatorInfo | None:
        """Get indicator by code."""
        return self._indicators.get(code)

    def list_categories(self) -> list[str]:
        """List all available categories."""
        return list(INDICATOR_CATALOG.keys())

    def list_by_category(self, category: str) -> list[IndicatorInfo]:
        """List all indicators in a category."""
        return [i for i in self._indicators.values() if i.category == category.lower()]

    def list_by_source(self, source: str) -> list[IndicatorInfo]:
        """List all indicators from a source."""
        return [i for i in self._indicators.values() if i.source == source.upper()]

    def to_dataframe(self) -> pd.DataFrame:
        """Export catalog as DataFrame."""
        return pd.DataFrame([i.to_dict() for i in self._indicators.values()])

    def sync_to_database(self, session: Session) -> int:
        """
        Synchronize catalog indicators to the database.

        Args:
            session: SQLAlchemy session.

        Returns:
            Number of indicators created/updated.
        """
        count = 0

        # Get sources
        sources = {s.code: s for s in session.query(Source).all()}
        categories = {c.code: c for c in session.query(Category).all()}

        for info in self._indicators.values():
            source = sources.get(info.source)
            category = categories.get(info.category.upper())

            existing = session.query(Indicator).filter_by(code=info.code).first()

            if existing:
                existing.name = info.name
                existing.description = info.description
                if source:
                    existing.source_id = source.id
                if category:
                    existing.category_id = category.id
            else:
                indicator = Indicator(
                    code=info.code,
                    name=info.name,
                    description=info.description,
                    source_id=source.id if source else None,
                    category_id=category.id if category else None,
                    frequency=info.frequency,
                )
                session.add(indicator)

            count += 1

        session.flush()
        return count


# Global catalog instance
catalog = IndicatorCatalog()


def search_indicators(
    query: str | None = None,
    category: str | None = None,
    source: str | None = None,
) -> pd.DataFrame:
    """
    Search indicators and return as DataFrame.

    Args:
        query: Search term.
        category: Filter by category.
        source: Filter by source (WB, IMF).

    Returns:
        DataFrame with matching indicators.
    """
    results = catalog.search(query, category, source)
    return pd.DataFrame([i.to_dict() for i in results])


def get_indicator_info(code: str) -> IndicatorInfo | None:
    """Get detailed info about an indicator."""
    return catalog.get(code)


def list_categories() -> list[str]:
    """List all indicator categories."""
    return catalog.list_categories()
