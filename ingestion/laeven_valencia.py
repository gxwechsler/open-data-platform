"""
IMF Laeven-Valencia Systemic Banking Crises Database Collector.

Source: IMF Working Paper WP/20/206
"Systemic Banking Crises Database II" by Luc Laeven and Fabián Valencia

Data includes:
- Banking crises (1970-2017+)
- Currency crises
- Sovereign debt crises
- Policy responses
- Fiscal costs and output losses

Download: https://www.imf.org/en/Publications/WP/Issues/2020/09/25/Systemic-Banking-Crises-Database-II-49733
"""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

import pandas as pd

from open_data.config import COUNTRIES, DataSource, settings


# =============================================================================
# CRISIS TYPE DEFINITIONS
# =============================================================================

@dataclass
class CrisisTypeDefinition:
    """Definition of a crisis type."""
    code: str
    name: str
    description: str


CRISIS_TYPES = {
    "BANKING": CrisisTypeDefinition(
        code="BANKING",
        name="Systemic Banking Crisis",
        description="Significant signs of financial distress in the banking system (bank runs, "
                    "losses in the banking system, bank liquidations) AND significant banking policy "
                    "intervention measures in response to significant losses in the banking system.",
    ),
    "CURRENCY": CrisisTypeDefinition(
        code="CURRENCY",
        name="Currency Crisis",
        description="A nominal depreciation of the currency of at least 30 percent that is also "
                    "at least 10 percentage points higher than the depreciation in the previous year.",
    ),
    "SOVEREIGN": CrisisTypeDefinition(
        code="SOVEREIGN",
        name="Sovereign Debt Crisis",
        description="Sovereign debt restructuring or sovereign default. This includes domestic "
                    "debt restructuring that is not a pure maturity extension.",
    ),
    "TWIN": CrisisTypeDefinition(
        code="TWIN",
        name="Twin Crisis (Banking + Currency)",
        description="Simultaneous occurrence of banking and currency crises.",
    ),
    "TRIPLE": CrisisTypeDefinition(
        code="TRIPLE",
        name="Triple Crisis (Banking + Currency + Sovereign)",
        description="Simultaneous occurrence of banking, currency, and sovereign debt crises.",
    ),
}


# =============================================================================
# LAEVEN-VALENCIA DATA STRUCTURE
# =============================================================================

# Column mappings from Excel to our database fields
BANKING_CRISIS_COLUMNS = {
    # Identification
    "Country": "country_name",
    "ISO": "iso3",
    "Banking Crisis": "start_year",
    "End": "end_year",
    
    # Costs
    "Output Loss (% of GDP)": "output_loss_pct_gdp",
    "Fiscal Costs (% of GDP)": "fiscal_cost_pct_gdp",
    "Fiscal Costs Net (% of GDP)": "fiscal_cost_net_pct_gdp",
    
    # Banking specific
    "Peak NPL (%)": "peak_npl_pct",
    "Liquidity Support (% of Deposits)": "liquidity_support_pct_gdp",
    "Recapitalization Costs (% of GDP)": "recapitalization_cost_pct_gdp",
    "Asset Purchases (% of GDP)": "asset_purchases_pct_gdp",
    "Monetary Expansion (% of GDP)": "monetary_expansion_pct_gdp",
    
    # Policy responses
    "Deposit Freeze": "deposit_freeze",
    "Bank Holiday": "bank_holiday",
    "Blanket Guarantee": "blanket_guarantee",
    "Nationalization": "nationalization",
    "Bank Restructuring": "bank_restructuring",
    "IMF Program": "imf_program",
}

CURRENCY_CRISIS_COLUMNS = {
    "Country": "country_name",
    "ISO": "iso3",
    "Currency Crisis": "start_year",
    "Depreciation (%)": "currency_depreciation_pct",
}

SOVEREIGN_CRISIS_COLUMNS = {
    "Country": "country_name",
    "ISO": "iso3",
    "Debt Crisis": "start_year",
    "End": "end_year",
    "Restructuring": "debt_restructured",
    "Haircut (%)": "haircut_pct",
}


# =============================================================================
# SAMPLE DATA (for demonstration/offline use)
# =============================================================================

SAMPLE_BANKING_CRISES = [
    # Major banking crises from Laeven-Valencia database
    {"iso3": "ARG", "start_year": 1980, "end_year": 1982, "output_loss_pct_gdp": 58.2, "fiscal_cost_pct_gdp": 55.1, "peak_npl_pct": 9.0, "imf_program": True},
    {"iso3": "ARG", "start_year": 1989, "end_year": 1991, "output_loss_pct_gdp": 27.0, "fiscal_cost_pct_gdp": 6.0, "peak_npl_pct": 27.0, "imf_program": True},
    {"iso3": "ARG", "start_year": 1995, "end_year": 1995, "output_loss_pct_gdp": None, "fiscal_cost_pct_gdp": 2.0, "peak_npl_pct": 12.3, "imf_program": True},
    {"iso3": "ARG", "start_year": 2001, "end_year": 2003, "output_loss_pct_gdp": 42.7, "fiscal_cost_pct_gdp": 9.6, "peak_npl_pct": 20.1, "deposit_freeze": True, "bank_holiday": True, "imf_program": True},
    {"iso3": "BRA", "start_year": 1990, "end_year": 1994, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 0.0, "peak_npl_pct": None, "imf_program": False},
    {"iso3": "BRA", "start_year": 1994, "end_year": 1998, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 13.2, "peak_npl_pct": 16.0, "imf_program": True},
    {"iso3": "CHL", "start_year": 1981, "end_year": 1985, "output_loss_pct_gdp": 92.4, "fiscal_cost_pct_gdp": 42.9, "peak_npl_pct": 35.6, "imf_program": True},
    {"iso3": "MEX", "start_year": 1981, "end_year": 1985, "output_loss_pct_gdp": 26.6, "fiscal_cost_pct_gdp": 0.0, "peak_npl_pct": None, "imf_program": True, "nationalization": True},
    {"iso3": "MEX", "start_year": 1994, "end_year": 1996, "output_loss_pct_gdp": 9.7, "fiscal_cost_pct_gdp": 19.3, "peak_npl_pct": 18.9, "imf_program": True},
    {"iso3": "USA", "start_year": 1988, "end_year": 1988, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 3.7, "peak_npl_pct": 4.1, "imf_program": False},
    {"iso3": "USA", "start_year": 2007, "end_year": 2011, "output_loss_pct_gdp": 31.0, "fiscal_cost_pct_gdp": 4.5, "peak_npl_pct": 5.0, "blanket_guarantee": False, "nationalization": True, "bank_restructuring": True},
    {"iso3": "GBR", "start_year": 2007, "end_year": 2011, "output_loss_pct_gdp": 23.8, "fiscal_cost_pct_gdp": 8.8, "peak_npl_pct": 4.0, "blanket_guarantee": True, "nationalization": True},
    {"iso3": "DEU", "start_year": 2008, "end_year": 2010, "output_loss_pct_gdp": 5.8, "fiscal_cost_pct_gdp": 1.8, "peak_npl_pct": 3.3, "nationalization": True},
    {"iso3": "FRA", "start_year": 2008, "end_year": 2009, "output_loss_pct_gdp": 23.0, "fiscal_cost_pct_gdp": 1.0, "peak_npl_pct": 4.0, "imf_program": False},
    {"iso3": "ESP", "start_year": 2008, "end_year": 2012, "output_loss_pct_gdp": 38.2, "fiscal_cost_pct_gdp": 5.4, "peak_npl_pct": 9.4, "nationalization": True, "bank_restructuring": True},
    {"iso3": "ITA", "start_year": 2008, "end_year": 2009, "output_loss_pct_gdp": 31.9, "fiscal_cost_pct_gdp": 0.3, "peak_npl_pct": 9.5, "imf_program": False},
    {"iso3": "NLD", "start_year": 2008, "end_year": 2009, "output_loss_pct_gdp": 22.5, "fiscal_cost_pct_gdp": 12.7, "peak_npl_pct": 3.2, "nationalization": True},
    {"iso3": "CHE", "start_year": 2008, "end_year": 2009, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 1.1, "peak_npl_pct": 0.8, "nationalization": True},
    {"iso3": "JPN", "start_year": 1997, "end_year": 2001, "output_loss_pct_gdp": 45.0, "fiscal_cost_pct_gdp": 14.0, "peak_npl_pct": 35.0, "blanket_guarantee": True, "nationalization": True},
    {"iso3": "CHN", "start_year": 1998, "end_year": 1998, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 18.0, "peak_npl_pct": 20.0, "nationalization": True, "bank_restructuring": True},
    {"iso3": "IND", "start_year": 1993, "end_year": 1993, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 0.0, "peak_npl_pct": 15.7, "nationalization": True},
    {"iso3": "TUR", "start_year": 2000, "end_year": 2001, "output_loss_pct_gdp": 18.3, "fiscal_cost_pct_gdp": 32.0, "peak_npl_pct": 27.6, "blanket_guarantee": True, "imf_program": True},
    {"iso3": "ZAF", "start_year": 1977, "end_year": 1977, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 0.0, "peak_npl_pct": None, "imf_program": False},
    {"iso3": "EGY", "start_year": 1980, "end_year": 1980, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 0.5, "peak_npl_pct": 25.0, "imf_program": True},
    {"iso3": "NGA", "start_year": 1991, "end_year": 1995, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 0.0, "peak_npl_pct": 77.0, "imf_program": False},
    {"iso3": "NGA", "start_year": 2009, "end_year": 2012, "output_loss_pct_gdp": 0.0, "fiscal_cost_pct_gdp": 11.8, "peak_npl_pct": 37.3, "nationalization": True},
    {"iso3": "AUS", "start_year": 1989, "end_year": 1992, "output_loss_pct_gdp": 2.0, "fiscal_cost_pct_gdp": 1.9, "peak_npl_pct": 6.0, "imf_program": False},
]

SAMPLE_CURRENCY_CRISES = [
    {"iso3": "ARG", "start_year": 1975, "currency_depreciation_pct": 99.0},
    {"iso3": "ARG", "start_year": 1981, "currency_depreciation_pct": 447.0},
    {"iso3": "ARG", "start_year": 1987, "currency_depreciation_pct": 149.0},
    {"iso3": "ARG", "start_year": 2002, "currency_depreciation_pct": 260.0},
    {"iso3": "ARG", "start_year": 2018, "currency_depreciation_pct": 103.0},
    {"iso3": "BRA", "start_year": 1983, "currency_depreciation_pct": 169.0},
    {"iso3": "BRA", "start_year": 1999, "currency_depreciation_pct": 78.0},
    {"iso3": "BRA", "start_year": 2015, "currency_depreciation_pct": 47.0},
    {"iso3": "MEX", "start_year": 1977, "currency_depreciation_pct": 45.0},
    {"iso3": "MEX", "start_year": 1982, "currency_depreciation_pct": 267.0},
    {"iso3": "MEX", "start_year": 1995, "currency_depreciation_pct": 101.0},
    {"iso3": "TUR", "start_year": 1978, "currency_depreciation_pct": 68.0},
    {"iso3": "TUR", "start_year": 1994, "currency_depreciation_pct": 163.0},
    {"iso3": "TUR", "start_year": 2001, "currency_depreciation_pct": 114.0},
    {"iso3": "TUR", "start_year": 2018, "currency_depreciation_pct": 67.0},
    {"iso3": "ZAF", "start_year": 1984, "currency_depreciation_pct": 54.0},
    {"iso3": "ZAF", "start_year": 2001, "currency_depreciation_pct": 37.0},
    {"iso3": "EGY", "start_year": 2016, "currency_depreciation_pct": 132.0},
    {"iso3": "NGA", "start_year": 1999, "currency_depreciation_pct": 38.0},
    {"iso3": "NGA", "start_year": 2016, "currency_depreciation_pct": 62.0},
    {"iso3": "IDN", "start_year": 1998, "currency_depreciation_pct": 244.0},
    {"iso3": "KOR", "start_year": 1998, "currency_depreciation_pct": 96.0},
    {"iso3": "THA", "start_year": 1998, "currency_depreciation_pct": 86.0},
    {"iso3": "RUS", "start_year": 1998, "currency_depreciation_pct": 245.0},
    {"iso3": "RUS", "start_year": 2014, "currency_depreciation_pct": 72.0},
]

SAMPLE_SOVEREIGN_CRISES = [
    {"iso3": "ARG", "start_year": 1982, "end_year": 1993, "debt_restructured": True, "haircut_pct": 30.0},
    {"iso3": "ARG", "start_year": 2001, "end_year": 2005, "debt_restructured": True, "haircut_pct": 73.0},
    {"iso3": "ARG", "start_year": 2020, "end_year": 2020, "debt_restructured": True, "haircut_pct": 45.0},
    {"iso3": "BRA", "start_year": 1983, "end_year": 1994, "debt_restructured": True, "haircut_pct": 25.0},
    {"iso3": "MEX", "start_year": 1982, "end_year": 1990, "debt_restructured": True, "haircut_pct": 35.0},
    {"iso3": "GRC", "start_year": 2012, "end_year": 2012, "debt_restructured": True, "haircut_pct": 53.5},
    {"iso3": "RUS", "start_year": 1998, "end_year": 2000, "debt_restructured": True, "haircut_pct": 50.0},
    {"iso3": "ECU", "start_year": 1999, "end_year": 2000, "debt_restructured": True, "haircut_pct": 38.0},
    {"iso3": "ECU", "start_year": 2008, "end_year": 2009, "debt_restructured": True, "haircut_pct": 68.0},
    {"iso3": "JAM", "start_year": 2010, "end_year": 2013, "debt_restructured": True, "haircut_pct": 11.0},
    {"iso3": "UKR", "start_year": 2015, "end_year": 2015, "debt_restructured": True, "haircut_pct": 20.0},
    {"iso3": "VEN", "start_year": 2017, "end_year": None, "debt_restructured": False, "haircut_pct": None},
]


# =============================================================================
# CRISIS INDICATORS (Early Warning Signals)
# =============================================================================

CRISIS_INDICATORS = {
    # Credit indicators
    "LV.CREDIT.GAP": {
        "name": "Credit-to-GDP Gap",
        "description": "Deviation of credit-to-GDP ratio from its long-term trend. Key early warning indicator for banking crises.",
        "unit": "percentage points",
        "source": "BIS/IMF",
    },
    "LV.CREDIT.GROWTH": {
        "name": "Real Credit Growth",
        "description": "Annual growth rate of real credit to the private sector.",
        "unit": "percent",
        "source": "IMF IFS",
    },
    "LV.CREDIT.GDP": {
        "name": "Credit-to-GDP Ratio",
        "description": "Total credit to the private non-financial sector as percentage of GDP.",
        "unit": "percent of GDP",
        "source": "BIS",
    },
    # Asset prices
    "LV.HOUSE.PRICE.GAP": {
        "name": "House Price Gap",
        "description": "Deviation of real house prices from long-term trend.",
        "unit": "percentage points",
        "source": "BIS",
    },
    "LV.EQUITY.RETURN": {
        "name": "Stock Market Return",
        "description": "Annual return of main stock market index.",
        "unit": "percent",
        "source": "Various",
    },
    # Banking sector
    "LV.NPL.RATIO": {
        "name": "Non-Performing Loans Ratio",
        "description": "Ratio of non-performing loans to total loans.",
        "unit": "percent",
        "source": "IMF FSI",
    },
    "LV.BANK.ROA": {
        "name": "Bank Return on Assets",
        "description": "Return on assets for the banking sector.",
        "unit": "percent",
        "source": "IMF FSI",
    },
    "LV.BANK.CAR": {
        "name": "Bank Capital Adequacy Ratio",
        "description": "Regulatory capital to risk-weighted assets.",
        "unit": "percent",
        "source": "IMF FSI",
    },
    # External sector
    "LV.CA.GDP": {
        "name": "Current Account Balance",
        "description": "Current account balance as percentage of GDP.",
        "unit": "percent of GDP",
        "source": "IMF WEO",
    },
    "LV.REER.GAP": {
        "name": "Real Effective Exchange Rate Gap",
        "description": "Deviation of REER from equilibrium.",
        "unit": "percentage points",
        "source": "IMF",
    },
    "LV.RESERVES.IMPORTS": {
        "name": "Reserve Coverage",
        "description": "Foreign reserves in months of imports.",
        "unit": "months",
        "source": "IMF IFS",
    },
    # Fiscal
    "LV.GOVT.DEBT.GDP": {
        "name": "Government Debt-to-GDP",
        "description": "General government gross debt as percentage of GDP.",
        "unit": "percent of GDP",
        "source": "IMF WEO",
    },
    "LV.FISCAL.BALANCE": {
        "name": "Fiscal Balance",
        "description": "General government net lending/borrowing as percentage of GDP.",
        "unit": "percent of GDP",
        "source": "IMF WEO",
    },
}


# =============================================================================
# DATA LOADING FUNCTIONS
# =============================================================================

class LaevenValenciaCollector:
    """
    Collector for IMF Laeven-Valencia Systemic Banking Crises Database.
    
    The database is typically distributed as an Excel file with the IMF working paper.
    This collector can:
    1. Load from a local Excel file
    2. Use built-in sample data for demonstration
    
    Usage:
        collector = LaevenValenciaCollector()
        
        # Load from sample data
        crises = collector.get_all_crises()
        
        # Load from Excel file
        crises = collector.load_from_excel("path/to/laeven_valencia.xlsx")
    """
    
    source_code = "LV"
    source_name = "IMF Laeven-Valencia"
    base_url = "https://www.imf.org/en/Publications/WP/Issues/2020/09/25/Systemic-Banking-Crises-Database-II-49733"
    
    def __init__(self):
        """Initialize the collector."""
        self.crisis_types = CRISIS_TYPES
        self.indicators = CRISIS_INDICATORS
    
    def get_crisis_types(self) -> list[dict[str, Any]]:
        """Get all crisis type definitions."""
        return [
            {
                "code": ct.code,
                "name": ct.name,
                "description": ct.description,
            }
            for ct in self.crisis_types.values()
        ]
    
    def get_banking_crises(self, use_sample: bool = True) -> pd.DataFrame:
        """
        Get banking crises data.
        
        Args:
            use_sample: If True, use built-in sample data.
            
        Returns:
            DataFrame with banking crisis records.
        """
        if use_sample:
            df = pd.DataFrame(SAMPLE_BANKING_CRISES)
            df["crisis_type"] = "BANKING"
            return df
        else:
            raise NotImplementedError("Excel loading requires file path. Use load_from_excel().")
    
    def get_currency_crises(self, use_sample: bool = True) -> pd.DataFrame:
        """
        Get currency crises data.
        
        Args:
            use_sample: If True, use built-in sample data.
            
        Returns:
            DataFrame with currency crisis records.
        """
        if use_sample:
            df = pd.DataFrame(SAMPLE_CURRENCY_CRISES)
            df["crisis_type"] = "CURRENCY"
            return df
        else:
            raise NotImplementedError("Excel loading requires file path. Use load_from_excel().")
    
    def get_sovereign_crises(self, use_sample: bool = True) -> pd.DataFrame:
        """
        Get sovereign debt crises data.
        
        Args:
            use_sample: If True, use built-in sample data.
            
        Returns:
            DataFrame with sovereign crisis records.
        """
        if use_sample:
            df = pd.DataFrame(SAMPLE_SOVEREIGN_CRISES)
            df["crisis_type"] = "SOVEREIGN"
            return df
        else:
            raise NotImplementedError("Excel loading requires file path. Use load_from_excel().")
    
    def get_all_crises(self, use_sample: bool = True) -> pd.DataFrame:
        """
        Get all crises combined.
        
        Args:
            use_sample: If True, use built-in sample data.
            
        Returns:
            DataFrame with all crisis records.
        """
        banking = self.get_banking_crises(use_sample)
        currency = self.get_currency_crises(use_sample)
        sovereign = self.get_sovereign_crises(use_sample)
        
        # Combine
        all_crises = pd.concat([banking, currency, sovereign], ignore_index=True)
        
        # Add country names
        all_crises["country_name"] = all_crises["iso3"].map(
            lambda x: COUNTRIES.get(x, {}).name if hasattr(COUNTRIES.get(x, {}), 'name') 
            else COUNTRIES.get(x, {}).get("name", x) if isinstance(COUNTRIES.get(x, {}), dict)
            else x
        )
        
        return all_crises.sort_values(["iso3", "start_year"])
    
    def load_from_excel(
        self,
        filepath: str | Path,
        sheet_banking: str = "Banking Crises",
        sheet_currency: str = "Currency Crises",
        sheet_sovereign: str = "Sovereign Debt Crises",
    ) -> dict[str, pd.DataFrame]:
        """
        Load crisis data from the official IMF Excel file.
        
        Args:
            filepath: Path to the Laeven-Valencia Excel file.
            sheet_banking: Name of the banking crises sheet.
            sheet_currency: Name of the currency crises sheet.
            sheet_sovereign: Name of the sovereign crises sheet.
            
        Returns:
            Dictionary with DataFrames for each crisis type.
        """
        filepath = Path(filepath)
        if not filepath.exists():
            raise FileNotFoundError(f"Excel file not found: {filepath}")
        
        result = {}
        
        try:
            # Load banking crises
            banking_df = pd.read_excel(filepath, sheet_name=sheet_banking)
            banking_df = self._process_banking_sheet(banking_df)
            result["banking"] = banking_df
        except Exception as e:
            print(f"Warning: Could not load banking crises: {e}")
            result["banking"] = pd.DataFrame()
        
        try:
            # Load currency crises
            currency_df = pd.read_excel(filepath, sheet_name=sheet_currency)
            currency_df = self._process_currency_sheet(currency_df)
            result["currency"] = currency_df
        except Exception as e:
            print(f"Warning: Could not load currency crises: {e}")
            result["currency"] = pd.DataFrame()
        
        try:
            # Load sovereign crises
            sovereign_df = pd.read_excel(filepath, sheet_name=sheet_sovereign)
            sovereign_df = self._process_sovereign_sheet(sovereign_df)
            result["sovereign"] = sovereign_df
        except Exception as e:
            print(f"Warning: Could not load sovereign crises: {e}")
            result["sovereign"] = pd.DataFrame()
        
        return result
    
    def _process_banking_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the banking crises sheet."""
        # Rename columns based on mapping
        df = df.rename(columns={k: v for k, v in BANKING_CRISIS_COLUMNS.items() if k in df.columns})
        df["crisis_type"] = "BANKING"
        return df
    
    def _process_currency_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the currency crises sheet."""
        df = df.rename(columns={k: v for k, v in CURRENCY_CRISIS_COLUMNS.items() if k in df.columns})
        df["crisis_type"] = "CURRENCY"
        return df
    
    def _process_sovereign_sheet(self, df: pd.DataFrame) -> pd.DataFrame:
        """Process the sovereign crises sheet."""
        df = df.rename(columns={k: v for k, v in SOVEREIGN_CRISIS_COLUMNS.items() if k in df.columns})
        df["crisis_type"] = "SOVEREIGN"
        return df
    
    def get_crisis_indicators(self) -> list[dict[str, Any]]:
        """Get crisis early warning indicator definitions."""
        return [
            {
                "code": code,
                "name": info["name"],
                "description": info["description"],
                "unit": info["unit"],
                "source": info["source"],
            }
            for code, info in self.indicators.items()
        ]
    
    def get_country_crisis_history(self, iso3: str) -> pd.DataFrame:
        """
        Get crisis history for a specific country.
        
        Args:
            iso3: ISO3 country code.
            
        Returns:
            DataFrame with all crises for the country.
        """
        all_crises = self.get_all_crises()
        return all_crises[all_crises["iso3"] == iso3.upper()].sort_values("start_year")
    
    def get_crisis_statistics(self) -> dict[str, Any]:
        """
        Get summary statistics for the crisis database.
        
        Returns:
            Dictionary with summary statistics.
        """
        all_crises = self.get_all_crises()
        
        return {
            "total_crises": len(all_crises),
            "banking_crises": len(all_crises[all_crises["crisis_type"] == "BANKING"]),
            "currency_crises": len(all_crises[all_crises["crisis_type"] == "CURRENCY"]),
            "sovereign_crises": len(all_crises[all_crises["crisis_type"] == "SOVEREIGN"]),
            "countries_affected": all_crises["iso3"].nunique(),
            "year_range": (all_crises["start_year"].min(), all_crises["start_year"].max()),
            "avg_output_loss": all_crises["output_loss_pct_gdp"].mean() if "output_loss_pct_gdp" in all_crises else None,
            "avg_fiscal_cost": all_crises["fiscal_cost_pct_gdp"].mean() if "fiscal_cost_pct_gdp" in all_crises else None,
        }
    
    def get_crises_by_decade(self) -> pd.DataFrame:
        """
        Get crisis counts by decade.
        
        Returns:
            DataFrame with crisis counts per decade.
        """
        all_crises = self.get_all_crises()
        all_crises["decade"] = (all_crises["start_year"] // 10) * 10
        
        return all_crises.groupby(["decade", "crisis_type"]).size().unstack(fill_value=0)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_crises(
    crisis_type: str | None = None,
    country: str | None = None,
    start_year: int | None = None,
    end_year: int | None = None,
) -> pd.DataFrame:
    """
    Get crisis data with optional filters.
    
    Args:
        crisis_type: Filter by crisis type (BANKING, CURRENCY, SOVEREIGN).
        country: Filter by ISO3 country code.
        start_year: Filter crises starting from this year.
        end_year: Filter crises starting up to this year.
        
    Returns:
        Filtered DataFrame of crises.
    """
    collector = LaevenValenciaCollector()
    df = collector.get_all_crises()
    
    if crisis_type:
        df = df[df["crisis_type"] == crisis_type.upper()]
    
    if country:
        df = df[df["iso3"] == country.upper()]
    
    if start_year:
        df = df[df["start_year"] >= start_year]
    
    if end_year:
        df = df[df["start_year"] <= end_year]
    
    return df


def get_banking_crisis_costs() -> pd.DataFrame:
    """
    Get banking crises with cost data.
    
    Returns:
        DataFrame with banking crises and their fiscal/output costs.
    """
    collector = LaevenValenciaCollector()
    df = collector.get_banking_crises()
    
    # Select relevant columns
    cols = ["iso3", "start_year", "end_year", "output_loss_pct_gdp", 
            "fiscal_cost_pct_gdp", "peak_npl_pct"]
    cols = [c for c in cols if c in df.columns]
    
    return df[cols].sort_values("output_loss_pct_gdp", ascending=False)


if __name__ == "__main__":
    # Demo usage
    collector = LaevenValenciaCollector()
    
    print("=" * 60)
    print("IMF Laeven-Valencia Crisis Database")
    print("=" * 60)
    
    # Get statistics
    stats = collector.get_crisis_statistics()
    print(f"\nDatabase Statistics:")
    print(f"  Total crises: {stats['total_crises']}")
    print(f"  Banking crises: {stats['banking_crises']}")
    print(f"  Currency crises: {stats['currency_crises']}")
    print(f"  Sovereign crises: {stats['sovereign_crises']}")
    print(f"  Countries affected: {stats['countries_affected']}")
    print(f"  Year range: {stats['year_range'][0]} - {stats['year_range'][1]}")
    
    # Get Argentina's crisis history
    print(f"\n\nArgentina Crisis History:")
    print("-" * 40)
    arg_crises = collector.get_country_crisis_history("ARG")
    for _, crisis in arg_crises.iterrows():
        end = crisis.get("end_year", "ongoing")
        crisis_type = crisis["crisis_type"]
        print(f"  {crisis['start_year']}-{end}: {crisis_type}")
    
    # Get crises by decade
    print(f"\n\nCrises by Decade:")
    print("-" * 40)
    by_decade = collector.get_crises_by_decade()
    print(by_decade)
