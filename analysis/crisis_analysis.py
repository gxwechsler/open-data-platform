"""
Advanced Crisis Analysis Module.

Features:
- Twin/Triple crisis detection (overlapping crisis types)
- Contagion analysis (crisis spreading between countries)
- Early warning indicators and anticipation signals

Based on academic research including:
- Kaminsky & Reinhart (1999) "The Twin Crises"
- Forbes & Rigobon (2002) "No Contagion, Only Interdependence"
- Borio & Lowe (2002) "Asset prices, financial and monetary stability"
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Any

import pandas as pd
import numpy as np


# =============================================================================
# DATA STRUCTURES
# =============================================================================

@dataclass
class TwinCrisis:
    """A twin or triple crisis event."""
    country: str
    country_name: str
    crisis_types: list[str]
    start_year: int
    end_year: int | None
    overlap_years: list[int]
    severity_score: float
    details: dict[str, Any] = field(default_factory=dict)
    
    @property
    def is_twin(self) -> bool:
        return len(self.crisis_types) == 2
    
    @property
    def is_triple(self) -> bool:
        return len(self.crisis_types) >= 3
    
    @property
    def classification(self) -> str:
        if len(self.crisis_types) >= 3:
            return "Triple Crisis"
        elif len(self.crisis_types) == 2:
            types = sorted(self.crisis_types)
            if "BANKING" in types and "CURRENCY" in types:
                return "Twin Crisis (Banking + Currency)"
            elif "BANKING" in types and "SOVEREIGN" in types:
                return "Twin Crisis (Banking + Sovereign)"
            elif "CURRENCY" in types and "SOVEREIGN" in types:
                return "Twin Crisis (Currency + Sovereign)"
        return "Single Crisis"


@dataclass
class ContagionEvent:
    """A potential contagion event between countries."""
    source_country: str
    source_country_name: str
    target_country: str
    target_country_name: str
    crisis_type: str
    source_year: int
    target_year: int
    lag_months: int
    contagion_probability: float
    channel: str  # trade, financial, regional, global
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class EarlyWarningSignal:
    """An early warning signal for potential crisis."""
    country: str
    country_name: str
    indicator: str
    indicator_name: str
    year: int
    value: float
    threshold: float
    breach_severity: float  # How much above/below threshold
    signal_type: str  # "warning", "alert", "critical"
    historical_accuracy: float  # How often this signal preceded crises
    
    @property
    def is_critical(self) -> bool:
        return self.signal_type == "critical"


# =============================================================================
# CRISIS DATA
# =============================================================================

COUNTRIES = {
    "ARG": "Argentina", "BRA": "Brazil", "CHL": "Chile", "COL": "Colombia",
    "MEX": "Mexico", "USA": "United States", "CAN": "Canada",
    "DEU": "Germany", "FRA": "France", "ITA": "Italy", "GBR": "United Kingdom",
    "ESP": "Spain", "NLD": "Netherlands", "CHE": "Switzerland",
    "SWE": "Sweden", "NOR": "Norway", "DNK": "Denmark", "FIN": "Finland",
    "TUR": "Turkey", "GRC": "Greece", "PRT": "Portugal", "IRL": "Ireland",
    "CHN": "China", "JPN": "Japan", "IND": "India", "SGP": "Singapore",
    "IDN": "Indonesia", "KOR": "South Korea", "THA": "Thailand", "MYS": "Malaysia",
    "PHL": "Philippines", "VNM": "Vietnam",
    "ISR": "Israel", "SAU": "Saudi Arabia", "ARE": "UAE",
    "ZAF": "South Africa", "EGY": "Egypt", "NGA": "Nigeria", "KEN": "Kenya",
    "AUS": "Australia", "NZL": "New Zealand",
    "RUS": "Russia", "UKR": "Ukraine", "VEN": "Venezuela", "ECU": "Ecuador",
}

# Regional groupings for contagion analysis
REGIONS = {
    "LATAM": ["ARG", "BRA", "CHL", "COL", "MEX", "VEN", "ECU"],
    "EUROPE_CORE": ["DEU", "FRA", "NLD", "CHE", "GBR"],
    "EUROPE_PERIPHERY": ["ESP", "ITA", "PRT", "GRC", "IRL"],
    "NORDIC": ["SWE", "NOR", "DNK", "FIN"],
    "ASIA_DEVELOPED": ["JPN", "KOR", "SGP"],
    "ASIA_EMERGING": ["CHN", "IND", "IDN", "THA", "MYS", "PHL", "VNM"],
    "MIDDLE_EAST": ["ISR", "SAU", "ARE", "TUR"],
    "AFRICA": ["ZAF", "EGY", "NGA", "KEN"],
    "OCEANIA": ["AUS", "NZL"],
    "CIS": ["RUS", "UKR"],
    "NORTH_AMERICA": ["USA", "CAN"],
}

# Trade linkages (simplified - major trading partners)
TRADE_LINKS = {
    "ARG": ["BRA", "USA", "CHN"],
    "BRA": ["ARG", "USA", "CHN", "DEU"],
    "MEX": ["USA", "CAN", "CHN"],
    "USA": ["CAN", "MEX", "CHN", "JPN", "DEU", "GBR"],
    "DEU": ["FRA", "NLD", "USA", "CHN", "ITA"],
    "GBR": ["USA", "DEU", "FRA", "NLD", "IRL"],
    "JPN": ["USA", "CHN", "KOR"],
    "CHN": ["USA", "JPN", "KOR", "DEU"],
    "KOR": ["CHN", "USA", "JPN"],
    "IDN": ["CHN", "JPN", "SGP", "USA"],
    "THA": ["CHN", "JPN", "USA", "MYS"],
    "MYS": ["CHN", "SGP", "USA", "JPN"],
    "RUS": ["CHN", "DEU", "NLD", "TUR"],
}

# Financial linkages (major financial centers exposure)
FINANCIAL_LINKS = {
    "USA": ["GBR", "JPN", "DEU", "FRA", "CAN", "CHE"],
    "GBR": ["USA", "DEU", "FRA", "NLD", "IRL", "CHE"],
    "DEU": ["USA", "GBR", "FRA", "NLD", "CHE", "ITA", "ESP"],
    "JPN": ["USA", "GBR", "CHN", "SGP"],
    "CHE": ["USA", "GBR", "DEU"],
    "SGP": ["USA", "GBR", "JPN", "CHN", "IDN", "MYS"],
}


# =============================================================================
# SAMPLE CRISIS DATA (Extended)
# =============================================================================

BANKING_CRISES = [
    {"iso3": "ARG", "start_year": 1980, "end_year": 1982, "output_loss": 58.2, "fiscal_cost": 55.1},
    {"iso3": "ARG", "start_year": 1989, "end_year": 1991, "output_loss": 27.0, "fiscal_cost": 6.0},
    {"iso3": "ARG", "start_year": 1995, "end_year": 1995, "output_loss": None, "fiscal_cost": 2.0},
    {"iso3": "ARG", "start_year": 2001, "end_year": 2003, "output_loss": 42.7, "fiscal_cost": 9.6},
    {"iso3": "BRA", "start_year": 1990, "end_year": 1994, "output_loss": 0.0, "fiscal_cost": 0.0},
    {"iso3": "BRA", "start_year": 1994, "end_year": 1998, "output_loss": 0.0, "fiscal_cost": 13.2},
    {"iso3": "CHL", "start_year": 1981, "end_year": 1985, "output_loss": 92.4, "fiscal_cost": 42.9},
    {"iso3": "MEX", "start_year": 1981, "end_year": 1985, "output_loss": 26.6, "fiscal_cost": 0.0},
    {"iso3": "MEX", "start_year": 1994, "end_year": 1996, "output_loss": 9.7, "fiscal_cost": 19.3},
    {"iso3": "USA", "start_year": 1988, "end_year": 1988, "output_loss": 0.0, "fiscal_cost": 3.7},
    {"iso3": "USA", "start_year": 2007, "end_year": 2011, "output_loss": 31.0, "fiscal_cost": 4.5},
    {"iso3": "GBR", "start_year": 2007, "end_year": 2011, "output_loss": 23.8, "fiscal_cost": 8.8},
    {"iso3": "DEU", "start_year": 2008, "end_year": 2010, "output_loss": 5.8, "fiscal_cost": 1.8},
    {"iso3": "FRA", "start_year": 2008, "end_year": 2009, "output_loss": 23.0, "fiscal_cost": 1.0},
    {"iso3": "ESP", "start_year": 2008, "end_year": 2012, "output_loss": 38.2, "fiscal_cost": 5.4},
    {"iso3": "ITA", "start_year": 2008, "end_year": 2009, "output_loss": 31.9, "fiscal_cost": 0.3},
    {"iso3": "NLD", "start_year": 2008, "end_year": 2009, "output_loss": 22.5, "fiscal_cost": 12.7},
    {"iso3": "IRL", "start_year": 2008, "end_year": 2012, "output_loss": 106.0, "fiscal_cost": 40.7},
    {"iso3": "GRC", "start_year": 2008, "end_year": 2012, "output_loss": 43.0, "fiscal_cost": 27.3},
    {"iso3": "PRT", "start_year": 2008, "end_year": 2012, "output_loss": 35.0, "fiscal_cost": 8.0},
    {"iso3": "JPN", "start_year": 1997, "end_year": 2001, "output_loss": 45.0, "fiscal_cost": 14.0},
    {"iso3": "CHN", "start_year": 1998, "end_year": 1998, "output_loss": 0.0, "fiscal_cost": 18.0},
    {"iso3": "IDN", "start_year": 1997, "end_year": 2001, "output_loss": 69.0, "fiscal_cost": 56.8},
    {"iso3": "KOR", "start_year": 1997, "end_year": 1998, "output_loss": 50.1, "fiscal_cost": 31.2},
    {"iso3": "THA", "start_year": 1997, "end_year": 2000, "output_loss": 109.3, "fiscal_cost": 43.8},
    {"iso3": "MYS", "start_year": 1997, "end_year": 1999, "output_loss": 31.4, "fiscal_cost": 16.4},
    {"iso3": "PHL", "start_year": 1997, "end_year": 2001, "output_loss": 0.0, "fiscal_cost": 13.2},
    {"iso3": "TUR", "start_year": 2000, "end_year": 2001, "output_loss": 18.3, "fiscal_cost": 32.0},
    {"iso3": "RUS", "start_year": 1998, "end_year": 1998, "output_loss": 0.0, "fiscal_cost": 0.0},
    {"iso3": "NGA", "start_year": 2009, "end_year": 2012, "output_loss": 0.0, "fiscal_cost": 11.8},
]

CURRENCY_CRISES = [
    {"iso3": "ARG", "start_year": 1975, "depreciation": 99.0},
    {"iso3": "ARG", "start_year": 1981, "depreciation": 447.0},
    {"iso3": "ARG", "start_year": 1987, "depreciation": 149.0},
    {"iso3": "ARG", "start_year": 2002, "depreciation": 260.0},
    {"iso3": "ARG", "start_year": 2018, "depreciation": 103.0},
    {"iso3": "BRA", "start_year": 1983, "depreciation": 169.0},
    {"iso3": "BRA", "start_year": 1999, "depreciation": 78.0},
    {"iso3": "BRA", "start_year": 2015, "depreciation": 47.0},
    {"iso3": "MEX", "start_year": 1977, "depreciation": 45.0},
    {"iso3": "MEX", "start_year": 1982, "depreciation": 267.0},
    {"iso3": "MEX", "start_year": 1995, "depreciation": 101.0},
    {"iso3": "CHL", "start_year": 1982, "depreciation": 89.0},
    {"iso3": "TUR", "start_year": 1994, "depreciation": 163.0},
    {"iso3": "TUR", "start_year": 2001, "depreciation": 114.0},
    {"iso3": "TUR", "start_year": 2018, "depreciation": 67.0},
    {"iso3": "IDN", "start_year": 1998, "depreciation": 244.0},
    {"iso3": "KOR", "start_year": 1998, "depreciation": 96.0},
    {"iso3": "THA", "start_year": 1997, "depreciation": 86.0},
    {"iso3": "MYS", "start_year": 1998, "depreciation": 65.0},
    {"iso3": "PHL", "start_year": 1998, "depreciation": 56.0},
    {"iso3": "RUS", "start_year": 1998, "depreciation": 245.0},
    {"iso3": "RUS", "start_year": 2014, "depreciation": 72.0},
    {"iso3": "UKR", "start_year": 2009, "depreciation": 52.0},
    {"iso3": "UKR", "start_year": 2014, "depreciation": 97.0},
    {"iso3": "VEN", "start_year": 2002, "depreciation": 93.0},
    {"iso3": "VEN", "start_year": 2010, "depreciation": 100.0},
    {"iso3": "GRC", "start_year": 2010, "depreciation": 0.0},  # Eurozone - no depreciation but crisis
    {"iso3": "IRL", "start_year": 2010, "depreciation": 0.0},
]

SOVEREIGN_CRISES = [
    {"iso3": "ARG", "start_year": 1982, "end_year": 1993, "haircut": 30.0},
    {"iso3": "ARG", "start_year": 2001, "end_year": 2005, "haircut": 73.0},
    {"iso3": "ARG", "start_year": 2020, "end_year": 2020, "haircut": 45.0},
    {"iso3": "BRA", "start_year": 1983, "end_year": 1994, "haircut": 25.0},
    {"iso3": "MEX", "start_year": 1982, "end_year": 1990, "haircut": 35.0},
    {"iso3": "CHL", "start_year": 1983, "end_year": 1990, "haircut": 20.0},
    {"iso3": "GRC", "start_year": 2012, "end_year": 2012, "haircut": 53.5},
    {"iso3": "RUS", "start_year": 1998, "end_year": 2000, "haircut": 50.0},
    {"iso3": "ECU", "start_year": 1999, "end_year": 2000, "haircut": 38.0},
    {"iso3": "ECU", "start_year": 2008, "end_year": 2009, "haircut": 68.0},
    {"iso3": "UKR", "start_year": 2015, "end_year": 2015, "haircut": 20.0},
    {"iso3": "VEN", "start_year": 2017, "end_year": None, "haircut": None},
    {"iso3": "IDN", "start_year": 1998, "end_year": 2000, "haircut": 10.0},
]


# =============================================================================
# EARLY WARNING INDICATORS
# =============================================================================

EARLY_WARNING_INDICATORS = {
    "CREDIT_GDP_GAP": {
        "name": "Credit-to-GDP Gap",
        "description": "Deviation of credit-to-GDP ratio from long-term trend (HP filter)",
        "threshold_warning": 2.0,
        "threshold_alert": 6.0,
        "threshold_critical": 10.0,
        "direction": "above",  # Crisis more likely when above threshold
        "lead_time_years": 2,
        "historical_accuracy": 0.72,
    },
    "CREDIT_GROWTH": {
        "name": "Real Credit Growth",
        "description": "Annual growth rate of real private sector credit",
        "threshold_warning": 10.0,
        "threshold_alert": 15.0,
        "threshold_critical": 20.0,
        "direction": "above",
        "lead_time_years": 2,
        "historical_accuracy": 0.65,
    },
    "HOUSE_PRICE_GAP": {
        "name": "Real House Price Gap",
        "description": "Deviation of real house prices from long-term trend",
        "threshold_warning": 10.0,
        "threshold_alert": 20.0,
        "threshold_critical": 30.0,
        "direction": "above",
        "lead_time_years": 2,
        "historical_accuracy": 0.68,
    },
    "CURRENT_ACCOUNT": {
        "name": "Current Account Balance",
        "description": "Current account as percentage of GDP",
        "threshold_warning": -4.0,
        "threshold_alert": -6.0,
        "threshold_critical": -8.0,
        "direction": "below",  # Crisis more likely when below threshold
        "lead_time_years": 1,
        "historical_accuracy": 0.58,
    },
    "REER_DEVIATION": {
        "name": "REER Overvaluation",
        "description": "Real effective exchange rate deviation from equilibrium",
        "threshold_warning": 10.0,
        "threshold_alert": 15.0,
        "threshold_critical": 25.0,
        "direction": "above",
        "lead_time_years": 1,
        "historical_accuracy": 0.62,
    },
    "RESERVE_COVERAGE": {
        "name": "Reserve Coverage",
        "description": "Foreign reserves in months of imports",
        "threshold_warning": 4.0,
        "threshold_alert": 3.0,
        "threshold_critical": 2.0,
        "direction": "below",
        "lead_time_years": 1,
        "historical_accuracy": 0.70,
    },
    "DEBT_GDP": {
        "name": "Government Debt-to-GDP",
        "description": "General government gross debt as % of GDP",
        "threshold_warning": 60.0,
        "threshold_alert": 90.0,
        "threshold_critical": 120.0,
        "direction": "above",
        "lead_time_years": 2,
        "historical_accuracy": 0.55,
    },
    "NPL_RATIO": {
        "name": "Non-Performing Loans",
        "description": "NPL as percentage of total loans",
        "threshold_warning": 5.0,
        "threshold_alert": 10.0,
        "threshold_critical": 15.0,
        "direction": "above",
        "lead_time_years": 1,
        "historical_accuracy": 0.75,
    },
    "BANK_LEVERAGE": {
        "name": "Bank Leverage Ratio",
        "description": "Banking sector assets to equity ratio",
        "threshold_warning": 15.0,
        "threshold_alert": 20.0,
        "threshold_critical": 25.0,
        "direction": "above",
        "lead_time_years": 2,
        "historical_accuracy": 0.60,
    },
    "STOCK_MARKET_BOOM": {
        "name": "Stock Market Boom",
        "description": "Real stock price growth (3-year cumulative)",
        "threshold_warning": 40.0,
        "threshold_alert": 60.0,
        "threshold_critical": 100.0,
        "direction": "above",
        "lead_time_years": 1,
        "historical_accuracy": 0.52,
    },
}


# Sample early warning data for demonstration
SAMPLE_EWI_DATA = [
    # Pre-Asian Crisis signals
    {"iso3": "THA", "year": 1995, "indicator": "CREDIT_GDP_GAP", "value": 15.2},
    {"iso3": "THA", "year": 1996, "indicator": "CREDIT_GDP_GAP", "value": 18.4},
    {"iso3": "THA", "year": 1995, "indicator": "CURRENT_ACCOUNT", "value": -8.1},
    {"iso3": "THA", "year": 1996, "indicator": "CURRENT_ACCOUNT", "value": -7.9},
    {"iso3": "KOR", "year": 1996, "indicator": "CREDIT_GDP_GAP", "value": 8.5},
    {"iso3": "KOR", "year": 1996, "indicator": "BANK_LEVERAGE", "value": 22.0},
    {"iso3": "IDN", "year": 1996, "indicator": "CREDIT_GDP_GAP", "value": 12.3},
    {"iso3": "MYS", "year": 1996, "indicator": "CREDIT_GDP_GAP", "value": 14.1},
    
    # Pre-GFC signals
    {"iso3": "USA", "year": 2005, "indicator": "CREDIT_GDP_GAP", "value": 4.2},
    {"iso3": "USA", "year": 2006, "indicator": "CREDIT_GDP_GAP", "value": 6.8},
    {"iso3": "USA", "year": 2006, "indicator": "HOUSE_PRICE_GAP", "value": 28.0},
    {"iso3": "USA", "year": 2007, "indicator": "HOUSE_PRICE_GAP", "value": 25.0},
    {"iso3": "GBR", "year": 2006, "indicator": "CREDIT_GDP_GAP", "value": 12.5},
    {"iso3": "GBR", "year": 2006, "indicator": "HOUSE_PRICE_GAP", "value": 35.0},
    {"iso3": "ESP", "year": 2006, "indicator": "CREDIT_GDP_GAP", "value": 18.2},
    {"iso3": "ESP", "year": 2006, "indicator": "HOUSE_PRICE_GAP", "value": 45.0},
    {"iso3": "IRL", "year": 2006, "indicator": "CREDIT_GDP_GAP", "value": 35.0},
    {"iso3": "IRL", "year": 2006, "indicator": "HOUSE_PRICE_GAP", "value": 55.0},
    
    # Pre-Euro crisis signals
    {"iso3": "GRC", "year": 2008, "indicator": "DEBT_GDP", "value": 109.0},
    {"iso3": "GRC", "year": 2009, "indicator": "DEBT_GDP", "value": 127.0},
    {"iso3": "GRC", "year": 2008, "indicator": "CURRENT_ACCOUNT", "value": -14.9},
    {"iso3": "PRT", "year": 2008, "indicator": "CURRENT_ACCOUNT", "value": -12.6},
    {"iso3": "ESP", "year": 2007, "indicator": "CURRENT_ACCOUNT", "value": -10.0},
    
    # Pre-Argentina 2001
    {"iso3": "ARG", "year": 1999, "indicator": "REER_DEVIATION", "value": 22.0},
    {"iso3": "ARG", "year": 2000, "indicator": "DEBT_GDP", "value": 45.0},
    {"iso3": "ARG", "year": 2000, "indicator": "CURRENT_ACCOUNT", "value": -3.2},
    
    # Current/Recent signals (for demo)
    {"iso3": "CHN", "year": 2023, "indicator": "CREDIT_GDP_GAP", "value": 25.0},
    {"iso3": "CHN", "year": 2023, "indicator": "HOUSE_PRICE_GAP", "value": -15.0},
    {"iso3": "TUR", "year": 2023, "indicator": "CURRENT_ACCOUNT", "value": -5.5},
    {"iso3": "TUR", "year": 2023, "indicator": "RESERVE_COVERAGE", "value": 2.8},
]


# =============================================================================
# ANALYSIS CLASSES
# =============================================================================

class CrisisAnalyzer:
    """
    Advanced crisis analysis including twin/triple detection,
    contagion analysis, and early warning signals.
    """
    
    def __init__(self):
        """Initialize with crisis data."""
        self.banking_crises = pd.DataFrame(BANKING_CRISES)
        self.currency_crises = pd.DataFrame(CURRENCY_CRISES)
        self.sovereign_crises = pd.DataFrame(SOVEREIGN_CRISES)
        self.ewi_data = pd.DataFrame(SAMPLE_EWI_DATA)
        self._prepare_data()
    
    def _prepare_data(self):
        """Prepare crisis data for analysis."""
        # Add crisis type labels
        self.banking_crises["crisis_type"] = "BANKING"
        self.currency_crises["crisis_type"] = "CURRENCY"
        self.sovereign_crises["crisis_type"] = "SOVEREIGN"
        
        # Ensure end_year exists
        if "end_year" not in self.currency_crises.columns:
            self.currency_crises["end_year"] = self.currency_crises["start_year"]
        
        # Combine all crises
        self.all_crises = pd.concat([
            self.banking_crises[["iso3", "start_year", "end_year", "crisis_type"]],
            self.currency_crises[["iso3", "start_year", "end_year", "crisis_type"]],
            self.sovereign_crises[["iso3", "start_year", "end_year", "crisis_type"]],
        ], ignore_index=True)
        
        # Fill missing end years
        self.all_crises["end_year"] = self.all_crises["end_year"].fillna(
            self.all_crises["start_year"]
        )
    
    # =========================================================================
    # TWIN/TRIPLE CRISIS DETECTION
    # =========================================================================
    
    def detect_twin_crises(self, overlap_window: int = 2) -> list[TwinCrisis]:
        """
        Detect twin and triple crises (overlapping crisis types).
        
        Args:
            overlap_window: Years of overlap to consider crises as "twin"
            
        Returns:
            List of TwinCrisis objects
        """
        twin_crises = []
        
        # Group crises by country
        for country in self.all_crises["iso3"].unique():
            country_crises = self.all_crises[self.all_crises["iso3"] == country]
            
            # Skip if only one crisis type
            if country_crises["crisis_type"].nunique() < 2:
                continue
            
            # Find overlapping periods
            overlaps = self._find_overlapping_crises(country_crises, overlap_window)
            
            for overlap in overlaps:
                twin = TwinCrisis(
                    country=country,
                    country_name=COUNTRIES.get(country, country),
                    crisis_types=overlap["types"],
                    start_year=overlap["start"],
                    end_year=overlap["end"],
                    overlap_years=overlap["overlap_years"],
                    severity_score=self._calculate_severity(country, overlap["types"], overlap["start"]),
                    details=overlap.get("details", {}),
                )
                twin_crises.append(twin)
        
        return sorted(twin_crises, key=lambda x: x.start_year)
    
    def _find_overlapping_crises(
        self, 
        country_crises: pd.DataFrame, 
        window: int
    ) -> list[dict]:
        """Find overlapping crisis periods for a country."""
        overlaps = []
        crises_list = country_crises.to_dict("records")
        
        # Check each pair of crises
        checked = set()
        
        for i, crisis1 in enumerate(crises_list):
            for j, crisis2 in enumerate(crises_list):
                if i >= j:
                    continue
                
                # Skip if same crisis type
                if crisis1["crisis_type"] == crisis2["crisis_type"]:
                    continue
                
                # Check for overlap
                start1, end1 = crisis1["start_year"], crisis1["end_year"]
                start2, end2 = crisis2["start_year"], crisis2["end_year"]
                
                # Calculate overlap
                overlap_start = max(start1, start2)
                overlap_end = min(end1, end2)
                
                # Check if within window
                if overlap_end - overlap_start >= -window:
                    key = tuple(sorted([
                        (crisis1["crisis_type"], start1),
                        (crisis2["crisis_type"], start2)
                    ]))
                    
                    if key not in checked:
                        checked.add(key)
                        
                        overlap_years = list(range(
                            max(start1, start2),
                            min(end1, end2) + 1
                        ))
                        
                        overlaps.append({
                            "types": sorted([crisis1["crisis_type"], crisis2["crisis_type"]]),
                            "start": min(start1, start2),
                            "end": max(end1, end2),
                            "overlap_years": overlap_years if overlap_years else [min(start1, start2)],
                        })
        
        # Check for triple crises (merge overlapping twins)
        merged = self._merge_into_triple(overlaps)
        
        return merged
    
    def _merge_into_triple(self, overlaps: list[dict]) -> list[dict]:
        """Merge twin crises that form triple crises."""
        if len(overlaps) < 2:
            return overlaps
        
        # Check if we can merge into triple
        all_types = set()
        all_years = set()
        
        for overlap in overlaps:
            all_types.update(overlap["types"])
            all_years.update(range(overlap["start"], overlap["end"] + 1))
        
        # If we have all three types with overlapping periods
        if len(all_types) == 3:
            # Find common overlap year
            year_counts = {}
            for overlap in overlaps:
                for year in range(overlap["start"], overlap["end"] + 1):
                    year_counts[year] = year_counts.get(year, 0) + 1
            
            # Check if any year has multiple overlaps
            triple_years = [y for y, c in year_counts.items() if c >= 2]
            
            if triple_years:
                return [{
                    "types": list(all_types),
                    "start": min(o["start"] for o in overlaps),
                    "end": max(o["end"] for o in overlaps),
                    "overlap_years": sorted(triple_years),
                }]
        
        return overlaps
    
    def _calculate_severity(
        self, 
        country: str, 
        crisis_types: list[str], 
        start_year: int
    ) -> float:
        """Calculate severity score for a crisis."""
        severity = 0.0
        
        # Banking crisis severity (output loss)
        if "BANKING" in crisis_types:
            banking = self.banking_crises[
                (self.banking_crises["iso3"] == country) &
                (self.banking_crises["start_year"] == start_year)
            ]
            if not banking.empty and pd.notna(banking.iloc[0].get("output_loss")):
                severity += banking.iloc[0]["output_loss"] / 100
        
        # Currency crisis severity (depreciation)
        if "CURRENCY" in crisis_types:
            currency = self.currency_crises[
                (self.currency_crises["iso3"] == country) &
                (abs(self.currency_crises["start_year"] - start_year) <= 1)
            ]
            if not currency.empty:
                severity += min(currency["depreciation"].max() / 200, 1.0)
        
        # Sovereign crisis severity (haircut)
        if "SOVEREIGN" in crisis_types:
            sovereign = self.sovereign_crises[
                (self.sovereign_crises["iso3"] == country) &
                (abs(self.sovereign_crises["start_year"] - start_year) <= 1)
            ]
            if not sovereign.empty and pd.notna(sovereign.iloc[0].get("haircut")):
                severity += sovereign.iloc[0]["haircut"] / 100
        
        # Multiply by number of crisis types
        return severity * len(crisis_types) / 3
    
    def get_twin_crisis_summary(self) -> pd.DataFrame:
        """Get summary of twin/triple crises."""
        twins = self.detect_twin_crises()
        
        rows = []
        for twin in twins:
            rows.append({
                "Country": twin.country_name,
                "ISO3": twin.country,
                "Type": twin.classification,
                "Start Year": twin.start_year,
                "End Year": twin.end_year,
                "Crisis Types": " + ".join(twin.crisis_types),
                "Severity Score": round(twin.severity_score, 2),
            })
        
        return pd.DataFrame(rows)
    
    # =========================================================================
    # CONTAGION ANALYSIS
    # =========================================================================
    
    def detect_contagion(
        self, 
        max_lag_months: int = 12,
        min_probability: float = 0.3
    ) -> list[ContagionEvent]:
        """
        Detect potential contagion events between countries.
        
        Args:
            max_lag_months: Maximum months between crises to consider contagion
            min_probability: Minimum probability threshold
            
        Returns:
            List of ContagionEvent objects
        """
        contagion_events = []
        
        # Group crises by type and year
        for crisis_type in ["BANKING", "CURRENCY", "SOVEREIGN"]:
            type_crises = self.all_crises[self.all_crises["crisis_type"] == crisis_type]
            
            # Sort by start year
            type_crises = type_crises.sort_values("start_year")
            
            # Look for sequential crises
            crises_list = type_crises.to_dict("records")
            
            for i, source_crisis in enumerate(crises_list):
                for j, target_crisis in enumerate(crises_list):
                    if i >= j:
                        continue
                    
                    # Check time lag
                    lag_years = target_crisis["start_year"] - source_crisis["start_year"]
                    if lag_years < 0 or lag_years > max_lag_months / 12:
                        continue
                    
                    # Skip same country
                    if source_crisis["iso3"] == target_crisis["iso3"]:
                        continue
                    
                    # Calculate contagion probability
                    probability, channel = self._calculate_contagion_probability(
                        source_crisis["iso3"],
                        target_crisis["iso3"],
                        lag_years,
                        crisis_type,
                    )
                    
                    if probability >= min_probability:
                        event = ContagionEvent(
                            source_country=source_crisis["iso3"],
                            source_country_name=COUNTRIES.get(source_crisis["iso3"], source_crisis["iso3"]),
                            target_country=target_crisis["iso3"],
                            target_country_name=COUNTRIES.get(target_crisis["iso3"], target_crisis["iso3"]),
                            crisis_type=crisis_type,
                            source_year=source_crisis["start_year"],
                            target_year=target_crisis["start_year"],
                            lag_months=int(lag_years * 12),
                            contagion_probability=probability,
                            channel=channel,
                        )
                        contagion_events.append(event)
        
        return sorted(contagion_events, key=lambda x: (x.source_year, -x.contagion_probability))
    
    def _calculate_contagion_probability(
        self,
        source: str,
        target: str,
        lag_years: float,
        crisis_type: str,
    ) -> tuple[float, str]:
        """Calculate probability that crisis spread via contagion."""
        probability = 0.0
        channel = "unknown"
        
        # Base probability decreases with lag
        time_factor = max(0, 1 - lag_years / 2)
        
        # Check regional contagion
        source_region = self._get_region(source)
        target_region = self._get_region(target)
        
        if source_region and source_region == target_region:
            probability = max(probability, 0.6 * time_factor)
            channel = "regional"
        
        # Check trade links
        if source in TRADE_LINKS and target in TRADE_LINKS.get(source, []):
            probability = max(probability, 0.7 * time_factor)
            channel = "trade"
        elif target in TRADE_LINKS and source in TRADE_LINKS.get(target, []):
            probability = max(probability, 0.7 * time_factor)
            channel = "trade"
        
        # Check financial links (strongest for banking crises)
        if crisis_type == "BANKING":
            if source in FINANCIAL_LINKS and target in FINANCIAL_LINKS.get(source, []):
                probability = max(probability, 0.8 * time_factor)
                channel = "financial"
            elif target in FINANCIAL_LINKS and source in FINANCIAL_LINKS.get(target, []):
                probability = max(probability, 0.8 * time_factor)
                channel = "financial"
        
        # Global crisis factor (2008, 1997-98)
        if source == "USA" and crisis_type == "BANKING":
            probability = max(probability, 0.9 * time_factor)
            channel = "global"
        
        return round(probability, 2), channel
    
    def _get_region(self, country: str) -> str | None:
        """Get region for a country."""
        for region, countries in REGIONS.items():
            if country in countries:
                return region
        return None
    
    def get_contagion_summary(self) -> pd.DataFrame:
        """Get summary of contagion events."""
        events = self.detect_contagion()
        
        rows = []
        for event in events:
            rows.append({
                "Source": event.source_country_name,
                "Target": event.target_country_name,
                "Crisis Type": event.crisis_type,
                "Source Year": event.source_year,
                "Target Year": event.target_year,
                "Lag (months)": event.lag_months,
                "Probability": event.contagion_probability,
                "Channel": event.channel,
            })
        
        return pd.DataFrame(rows)
    
    def get_contagion_network(self, year: int | None = None) -> dict:
        """
        Get contagion network data for visualization.
        
        Returns dict with nodes and edges for network graph.
        """
        events = self.detect_contagion()
        
        if year:
            events = [e for e in events if e.source_year == year]
        
        nodes = set()
        edges = []
        
        for event in events:
            nodes.add(event.source_country)
            nodes.add(event.target_country)
            edges.append({
                "source": event.source_country,
                "target": event.target_country,
                "weight": event.contagion_probability,
                "channel": event.channel,
                "year": event.source_year,
            })
        
        return {
            "nodes": [{"id": n, "name": COUNTRIES.get(n, n)} for n in nodes],
            "edges": edges,
        }
    
    def analyze_crisis_clusters(self) -> pd.DataFrame:
        """Analyze crisis clustering (multiple countries affected simultaneously)."""
        # Group by year
        yearly = self.all_crises.groupby("start_year").agg({
            "iso3": lambda x: list(x.unique()),
            "crisis_type": lambda x: list(x),
        }).reset_index()
        
        yearly["country_count"] = yearly["iso3"].apply(len)
        yearly["crisis_count"] = yearly["crisis_type"].apply(len)
        
        # Flag cluster years (3+ countries)
        yearly["is_cluster"] = yearly["country_count"] >= 3
        
        clusters = yearly[yearly["is_cluster"]].copy()
        clusters["countries"] = clusters["iso3"].apply(lambda x: ", ".join(x))
        
        return clusters[["start_year", "country_count", "crisis_count", "countries"]]
    
    # =========================================================================
    # EARLY WARNING SIGNALS
    # =========================================================================
    
    def get_early_warning_signals(
        self,
        year: int | None = None,
        country: str | None = None,
    ) -> list[EarlyWarningSignal]:
        """
        Get early warning signals based on indicator thresholds.
        
        Args:
            year: Filter by year
            country: Filter by country
            
        Returns:
            List of EarlyWarningSignal objects
        """
        signals = []
        
        data = self.ewi_data.copy()
        
        if year:
            data = data[data["year"] == year]
        if country:
            data = data[data["iso3"] == country.upper()]
        
        for _, row in data.iterrows():
            indicator_code = row["indicator"]
            if indicator_code not in EARLY_WARNING_INDICATORS:
                continue
            
            indicator = EARLY_WARNING_INDICATORS[indicator_code]
            value = row["value"]
            
            # Determine signal type
            signal_type, breach_severity = self._evaluate_signal(
                value,
                indicator["threshold_warning"],
                indicator["threshold_alert"],
                indicator["threshold_critical"],
                indicator["direction"],
            )
            
            if signal_type:
                signal = EarlyWarningSignal(
                    country=row["iso3"],
                    country_name=COUNTRIES.get(row["iso3"], row["iso3"]),
                    indicator=indicator_code,
                    indicator_name=indicator["name"],
                    year=row["year"],
                    value=value,
                    threshold=indicator[f"threshold_{signal_type}"],
                    breach_severity=breach_severity,
                    signal_type=signal_type,
                    historical_accuracy=indicator["historical_accuracy"],
                )
                signals.append(signal)
        
        return sorted(signals, key=lambda x: (-x.breach_severity, x.year))
    
    def _evaluate_signal(
        self,
        value: float,
        warning: float,
        alert: float,
        critical: float,
        direction: str,
    ) -> tuple[str | None, float]:
        """Evaluate which threshold is breached."""
        if direction == "above":
            if value >= critical:
                return "critical", (value - critical) / critical
            elif value >= alert:
                return "alert", (value - alert) / alert
            elif value >= warning:
                return "warning", (value - warning) / warning
        else:  # below
            if value <= critical:
                return "critical", (critical - value) / abs(critical) if critical != 0 else 1.0
            elif value <= alert:
                return "alert", (alert - value) / abs(alert) if alert != 0 else 1.0
            elif value <= warning:
                return "warning", (warning - value) / abs(warning) if warning != 0 else 1.0
        
        return None, 0.0
    
    def get_ewi_summary(self, year: int | None = None) -> pd.DataFrame:
        """Get summary of early warning signals."""
        signals = self.get_early_warning_signals(year=year)
        
        rows = []
        for signal in signals:
            rows.append({
                "Country": signal.country_name,
                "ISO3": signal.country,
                "Year": signal.year,
                "Indicator": signal.indicator_name,
                "Value": round(signal.value, 1),
                "Threshold": signal.threshold,
                "Signal": signal.signal_type.upper(),
                "Severity": round(signal.breach_severity, 2),
                "Historical Accuracy": f"{signal.historical_accuracy:.0%}",
            })
        
        return pd.DataFrame(rows)
    
    def get_country_risk_score(self, country: str, year: int) -> dict:
        """
        Calculate composite risk score for a country.
        
        Returns dict with overall score and breakdown by indicator.
        """
        signals = self.get_early_warning_signals(year=year, country=country)
        
        if not signals:
            return {
                "country": country,
                "year": year,
                "risk_score": 0.0,
                "risk_level": "Low",
                "signals": [],
            }
        
        # Calculate weighted risk score
        total_weight = 0.0
        weighted_score = 0.0
        
        signal_details = []
        
        for signal in signals:
            # Weight by historical accuracy
            weight = signal.historical_accuracy
            
            # Score based on signal type
            if signal.signal_type == "critical":
                score = 1.0
            elif signal.signal_type == "alert":
                score = 0.6
            else:
                score = 0.3
            
            weighted_score += score * weight * (1 + signal.breach_severity)
            total_weight += weight
            
            signal_details.append({
                "indicator": signal.indicator_name,
                "signal": signal.signal_type,
                "value": signal.value,
                "contribution": round(score * weight, 2),
            })
        
        risk_score = weighted_score / total_weight if total_weight > 0 else 0.0
        
        # Determine risk level
        if risk_score >= 0.7:
            risk_level = "Critical"
        elif risk_score >= 0.5:
            risk_level = "High"
        elif risk_score >= 0.3:
            risk_level = "Elevated"
        else:
            risk_level = "Moderate"
        
        return {
            "country": country,
            "country_name": COUNTRIES.get(country, country),
            "year": year,
            "risk_score": round(risk_score, 2),
            "risk_level": risk_level,
            "signals": signal_details,
            "signal_count": len(signals),
        }
    
    def get_crisis_prediction_accuracy(self) -> pd.DataFrame:
        """
        Analyze historical accuracy of early warning signals.
        
        Compares signals to actual crises to calculate hit rates.
        """
        results = []
        
        for indicator_code, indicator in EARLY_WARNING_INDICATORS.items():
            lead_time = indicator["lead_time_years"]
            
            # Get all signals for this indicator
            indicator_signals = self.ewi_data[self.ewi_data["indicator"] == indicator_code]
            
            true_positives = 0
            false_positives = 0
            total_signals = 0
            
            for _, signal in indicator_signals.iterrows():
                country = signal["iso3"]
                year = signal["year"]
                value = signal["value"]
                
                # Check if above warning threshold
                if indicator["direction"] == "above" and value < indicator["threshold_warning"]:
                    continue
                if indicator["direction"] == "below" and value > indicator["threshold_warning"]:
                    continue
                
                total_signals += 1
                
                # Check if crisis followed
                crisis_followed = self.all_crises[
                    (self.all_crises["iso3"] == country) &
                    (self.all_crises["start_year"] > year) &
                    (self.all_crises["start_year"] <= year + lead_time + 1)
                ]
                
                if not crisis_followed.empty:
                    true_positives += 1
                else:
                    false_positives += 1
            
            if total_signals > 0:
                results.append({
                    "Indicator": indicator["name"],
                    "Lead Time (years)": lead_time,
                    "Total Signals": total_signals,
                    "True Positives": true_positives,
                    "False Positives": false_positives,
                    "Hit Rate": f"{true_positives / total_signals:.1%}" if total_signals > 0 else "N/A",
                    "Stated Accuracy": f"{indicator['historical_accuracy']:.0%}",
                })
        
        return pd.DataFrame(results)


# =============================================================================
# CONVENIENCE FUNCTIONS
# =============================================================================

def get_twin_crises() -> pd.DataFrame:
    """Get all twin/triple crises."""
    analyzer = CrisisAnalyzer()
    return analyzer.get_twin_crisis_summary()


def get_contagion_events() -> pd.DataFrame:
    """Get all contagion events."""
    analyzer = CrisisAnalyzer()
    return analyzer.get_contagion_summary()


def get_early_warnings(year: int = None) -> pd.DataFrame:
    """Get early warning signals."""
    analyzer = CrisisAnalyzer()
    return analyzer.get_ewi_summary(year=year)


def get_country_risk(country: str, year: int = 2023) -> dict:
    """Get risk assessment for a country."""
    analyzer = CrisisAnalyzer()
    return analyzer.get_country_risk_score(country, year)


if __name__ == "__main__":
    # Demo
    analyzer = CrisisAnalyzer()
    
    print("=" * 70)
    print("ADVANCED CRISIS ANALYSIS")
    print("=" * 70)
    
    # Twin crises
    print("\n📊 TWIN/TRIPLE CRISES")
    print("-" * 50)
    twins = analyzer.get_twin_crisis_summary()
    print(twins.to_string(index=False))
    
    # Contagion
    print("\n\n🌐 CONTAGION EVENTS (Top 10)")
    print("-" * 50)
    contagion = analyzer.get_contagion_summary().head(10)
    print(contagion.to_string(index=False))
    
    # Crisis clusters
    print("\n\n📅 CRISIS CLUSTERS (3+ countries)")
    print("-" * 50)
    clusters = analyzer.analyze_crisis_clusters()
    print(clusters.to_string(index=False))
    
    # Early warning signals
    print("\n\n⚠️ EARLY WARNING SIGNALS (2006 - Pre-GFC)")
    print("-" * 50)
    ewi = analyzer.get_ewi_summary(year=2006)
    print(ewi.to_string(index=False))
    
    # Country risk
    print("\n\n🎯 COUNTRY RISK ASSESSMENT (2023)")
    print("-" * 50)
    for country in ["CHN", "TUR"]:
        risk = analyzer.get_country_risk_score(country, 2023)
        print(f"\n{risk['country_name']}:")
        print(f"  Risk Score: {risk['risk_score']}")
        print(f"  Risk Level: {risk['risk_level']}")
        for signal in risk['signals']:
            print(f"  - {signal['indicator']}: {signal['signal']} (contribution: {signal['contribution']})")
