"""
Configuration settings and constants for Open Data Platform.
"""

from enum import Enum
from pathlib import Path
from typing import NamedTuple

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


# =============================================================================
# PATHS
# =============================================================================

PROJECT_ROOT = Path(__file__).parent.parent.parent
DATA_DIR = PROJECT_ROOT / "data"
EXPORTS_DIR = PROJECT_ROOT / "exports"
CACHE_DIR = PROJECT_ROOT / ".cache"


# =============================================================================
# SETTINGS
# =============================================================================


class Settings(BaseSettings):
    """Application settings loaded from environment variables."""

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    # Database
    postgres_user: str = Field(default="opendata")
    postgres_password: str = Field(default="opendata_secret")
    postgres_host: str = Field(default="localhost")
    postgres_port: int = Field(default=5432)
    postgres_db: str = Field(default="open_data")

    # API Settings
    world_bank_api_base: str = Field(default="https://api.worldbank.org/v2")
    imf_api_base: str = Field(default="https://dataservices.imf.org/REST/SDMX_JSON.svc")
    request_timeout: int = Field(default=60)
    max_retries: int = Field(default=3)

    # Data Settings
    default_start_year: int = Field(default=1960)
    default_end_year: int = Field(default=2024)

    @property
    def database_url(self) -> str:
        """Construct PostgreSQL connection URL."""
        return (
            f"postgresql://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )

    @property
    def async_database_url(self) -> str:
        """Construct async PostgreSQL connection URL."""
        return (
            f"postgresql+asyncpg://{self.postgres_user}:{self.postgres_password}"
            f"@{self.postgres_host}:{self.postgres_port}/{self.postgres_db}"
        )


settings = Settings()


# =============================================================================
# ENUMS
# =============================================================================


class Region(str, Enum):
    """Geographic regions."""

    AMERICA = "AMERICA"
    EUROPE = "EUROPE"
    ASIA = "ASIA"
    MIDDLE_EAST = "MIDDLE_EAST"
    AFRICA = "AFRICA"
    SOUTH_PACIFIC = "SOUTH_PACIFIC"


class DataSource(str, Enum):
    """Available data sources."""

    WORLD_BANK = "WB"
    IMF = "IMF"
    UCDP = "UCDP"
    UNHCR = "UNHCR"
    IRENA = "IRENA"
    ITU = "ITU"
    UNODC = "UNODC"
    WRI = "WRI"


class Category(str, Enum):
    """Indicator categories."""

    ECONOMIC = "ECONOMIC"
    FINANCIAL = "FINANCIAL"
    DEMOGRAPHIC = "DEMOGRAPHIC"
    SOCIAL = "SOCIAL"
    HEALTH = "HEALTH"
    EDUCATION = "EDUCATION"
    ENVIRONMENT = "ENVIRONMENT"
    INFRASTRUCTURE = "INFRASTRUCTURE"
    GOVERNANCE = "GOVERNANCE"
    SECURITY = "SECURITY"


# =============================================================================
# COUNTRY DATA
# =============================================================================


class Country(NamedTuple):
    """Country information."""

    iso3: str
    iso2: str
    name: str
    region: Region
    subregion: str


# 45 countries organized by region
COUNTRIES: dict[str, Country] = {
    # AMERICA (7 countries)
    "ARG": Country("ARG", "AR", "Argentina", Region.AMERICA, "South America"),
    "BRA": Country("BRA", "BR", "Brazil", Region.AMERICA, "South America"),
    "CHL": Country("CHL", "CL", "Chile", Region.AMERICA, "South America"),
    "COL": Country("COL", "CO", "Colombia", Region.AMERICA, "South America"),
    "MEX": Country("MEX", "MX", "Mexico", Region.AMERICA, "North America"),
    "USA": Country("USA", "US", "United States", Region.AMERICA, "North America"),
    "CAN": Country("CAN", "CA", "Canada", Region.AMERICA, "North America"),
    # EUROPE (12 countries)
    "DEU": Country("DEU", "DE", "Germany", Region.EUROPE, "Western Europe"),
    "FRA": Country("FRA", "FR", "France", Region.EUROPE, "Western Europe"),
    "ITA": Country("ITA", "IT", "Italy", Region.EUROPE, "Southern Europe"),
    "SWE": Country("SWE", "SE", "Sweden", Region.EUROPE, "Northern Europe"),
    "NLD": Country("NLD", "NL", "Netherlands", Region.EUROPE, "Western Europe"),
    "CHE": Country("CHE", "CH", "Switzerland", Region.EUROPE, "Western Europe"),
    "DNK": Country("DNK", "DK", "Denmark", Region.EUROPE, "Northern Europe"),
    "FIN": Country("FIN", "FI", "Finland", Region.EUROPE, "Northern Europe"),
    "NOR": Country("NOR", "NO", "Norway", Region.EUROPE, "Northern Europe"),
    "TUR": Country("TUR", "TR", "Turkey", Region.EUROPE, "Southern Europe"),
    "ESP": Country("ESP", "ES", "Spain", Region.EUROPE, "Southern Europe"),
    "GBR": Country("GBR", "GB", "United Kingdom", Region.EUROPE, "Northern Europe"),
    # ASIA (5 countries)
    "IND": Country("IND", "IN", "India", Region.ASIA, "South Asia"),
    "CHN": Country("CHN", "CN", "China", Region.ASIA, "East Asia"),
    "JPN": Country("JPN", "JP", "Japan", Region.ASIA, "East Asia"),
    "VNM": Country("VNM", "VN", "Vietnam", Region.ASIA, "Southeast Asia"),
    "SGP": Country("SGP", "SG", "Singapore", Region.ASIA, "Southeast Asia"),
    # MIDDLE EAST (5 countries)
    "ISR": Country("ISR", "IL", "Israel", Region.MIDDLE_EAST, "Western Asia"),
    "IRN": Country("IRN", "IR", "Iran", Region.MIDDLE_EAST, "Western Asia"),
    "ARE": Country("ARE", "AE", "United Arab Emirates", Region.MIDDLE_EAST, "Arabian Peninsula"),
    "SAU": Country("SAU", "SA", "Saudi Arabia", Region.MIDDLE_EAST, "Arabian Peninsula"),
    "QAT": Country("QAT", "QA", "Qatar", Region.MIDDLE_EAST, "Arabian Peninsula"),
    # AFRICA (11 countries)
    "NER": Country("NER", "NE", "Niger", Region.AFRICA, "West Africa"),
    "ZAF": Country("ZAF", "ZA", "South Africa", Region.AFRICA, "Southern Africa"),
    "EGY": Country("EGY", "EG", "Egypt", Region.AFRICA, "North Africa"),
    "COD": Country("COD", "CD", "Congo (DRC)", Region.AFRICA, "Central Africa"),
    "MAR": Country("MAR", "MA", "Morocco", Region.AFRICA, "North Africa"),
    "DZA": Country("DZA", "DZ", "Algeria", Region.AFRICA, "North Africa"),
    "ETH": Country("ETH", "ET", "Ethiopia", Region.AFRICA, "East Africa"),
    "LBY": Country("LBY", "LY", "Libya", Region.AFRICA, "North Africa"),
    "TZA": Country("TZA", "TZ", "Tanzania", Region.AFRICA, "East Africa"),
    "TUN": Country("TUN", "TN", "Tunisia", Region.AFRICA, "North Africa"),
    "GHA": Country("GHA", "GH", "Ghana", Region.AFRICA, "West Africa"),
    # SOUTH PACIFIC (2 countries)
    "AUS": Country("AUS", "AU", "Australia", Region.SOUTH_PACIFIC, "Oceania"),
    "NZL": Country("NZL", "NZ", "New Zealand", Region.SOUTH_PACIFIC, "Oceania"),
}

# Quick lookup helpers
COUNTRY_CODES = list(COUNTRIES.keys())
ISO2_TO_ISO3 = {c.iso2: c.iso3 for c in COUNTRIES.values()}
ISO3_TO_ISO2 = {c.iso3: c.iso2 for c in COUNTRIES.values()}


def get_countries_by_region(region: Region) -> list[Country]:
    """Get all countries in a specific region."""
    return [c for c in COUNTRIES.values() if c.region == region]


def get_country(code: str) -> Country | None:
    """Get country by ISO3 or ISO2 code."""
    code = code.upper()
    if code in COUNTRIES:
        return COUNTRIES[code]
    if code in ISO2_TO_ISO3:
        return COUNTRIES[ISO2_TO_ISO3[code]]
    return None


# =============================================================================
# WORLD BANK INDICATORS (Economic/Financial - Phase 1)
# =============================================================================

# Key economic indicators from World Bank
WORLD_BANK_INDICATORS = {
    # GDP & Growth
    "NY.GDP.MKTP.CD": "GDP (current US$)",
    "NY.GDP.MKTP.KD": "GDP (constant 2015 US$)",
    "NY.GDP.MKTP.KD.ZG": "GDP growth (annual %)",
    "NY.GDP.PCAP.CD": "GDP per capita (current US$)",
    "NY.GDP.PCAP.KD": "GDP per capita (constant 2015 US$)",
    "NY.GDP.PCAP.KD.ZG": "GDP per capita growth (annual %)",
    "NY.GDP.PCAP.PP.CD": "GDP per capita, PPP (current international $)",
    # Trade
    "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
    "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
    "NE.TRD.GNFS.ZS": "Trade (% of GDP)",
    "BN.CAB.XOKA.CD": "Current account balance (BoP, current US$)",
    "BN.CAB.XOKA.GD.ZS": "Current account balance (% of GDP)",
    # Inflation & Prices
    "FP.CPI.TOTL.ZG": "Inflation, consumer prices (annual %)",
    "FP.CPI.TOTL": "Consumer price index (2010 = 100)",
    # Foreign Investment
    "BX.KLT.DINV.CD.WD": "Foreign direct investment, net inflows (BoP, current US$)",
    "BX.KLT.DINV.WD.GD.ZS": "Foreign direct investment, net inflows (% of GDP)",
    # Government Finance
    "GC.REV.XGRT.GD.ZS": "Revenue, excluding grants (% of GDP)",
    "GC.XPN.TOTL.GD.ZS": "Expense (% of GDP)",
    "GC.DOD.TOTL.GD.ZS": "Central government debt, total (% of GDP)",
    "GC.BAL.CASH.GD.ZS": "Cash surplus/deficit (% of GDP)",
    # External Debt
    "DT.DOD.DECT.CD": "External debt stocks, total (DOD, current US$)",
    "DT.DOD.DECT.GN.ZS": "External debt stocks (% of GNI)",
    # Unemployment & Labor
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of total labor force)",
    "SL.UEM.TOTL.NE.ZS": "Unemployment, total (% of total labor force) (national estimate)",
    # Population (for context)
    "SP.POP.TOTL": "Population, total",
    "SP.POP.GROW": "Population growth (annual %)",
    # Interest Rates
    "FR.INR.RINR": "Real interest rate (%)",
    "FR.INR.LEND": "Lending interest rate (%)",
    # Exchange Rate
    "PA.NUS.FCRF": "Official exchange rate (LCU per US$, period average)",
    # Health - Suicide
    "SH.STA.SUIC.P5": "Suicide mortality rate (per 100,000 population)",
    "SH.STA.SUIC.MA.P5": "Suicide mortality rate, male (per 100,000 male population)",
    "SH.STA.SUIC.FE.P5": "Suicide mortality rate, female (per 100,000 female population)",
}



# =============================================================================
# WORLD BANK HEALTH INDICATORS
# =============================================================================

WORLD_BANK_HEALTH_INDICATORS = {
    # Life Expectancy
    "SP.DYN.LE00.IN": "Life expectancy at birth, total (years)",
    "SP.DYN.LE00.MA.IN": "Life expectancy at birth, male (years)",
    "SP.DYN.LE00.FE.IN": "Life expectancy at birth, female (years)",
    # Mortality
    "SH.DYN.MORT": "Mortality rate, under-5 (per 1,000 live births)",
    "SH.DYN.NMRT": "Mortality rate, neonatal (per 1,000 live births)",
    "SP.DYN.IMRT.IN": "Mortality rate, infant (per 1,000 live births)",
    "SH.STA.MMRT": "Maternal mortality ratio (per 100,000 live births)",
    # Health System
    "SH.XPD.CHEX.PC.CD": "Health expenditure per capita (current US$)",
    "SH.XPD.CHEX.GD.ZS": "Health expenditure (% of GDP)",
    "SH.MED.PHYS.ZS": "Physicians (per 1,000 people)",
    "SH.MED.BEDS.ZS": "Hospital beds (per 1,000 people)",
    # Immunization & Disease
    "SH.IMM.IDPT": "Immunization, DPT (% of children ages 12-23 months)",
    "SH.TBS.INCD": "Tuberculosis incidence (per 100,000 people)",
    # Suicide (already loaded, included for completeness)
    "SH.STA.SUIC.P5": "Suicide mortality rate (per 100,000 population)",
    "SH.STA.SUIC.MA.P5": "Suicide mortality rate, male (per 100,000 male population)",
    "SH.STA.SUIC.FE.P5": "Suicide mortality rate, female (per 100,000 female population)",
}



# =============================================================================
# WORLD BANK EDUCATION INDICATORS
# =============================================================================

WORLD_BANK_EDUCATION_INDICATORS = {
    # Literacy
    "SE.ADT.LITR.ZS": "Literacy rate, adult total (% of people ages 15+)",
    "SE.ADT.LITR.MA.ZS": "Literacy rate, adult male (% of males ages 15+)",
    "SE.ADT.LITR.FE.ZS": "Literacy rate, adult female (% of females ages 15+)",
    # School Enrollment
    "SE.PRM.ENRR": "School enrollment, primary (% gross)",
    "SE.SEC.ENRR": "School enrollment, secondary (% gross)",
    "SE.TER.ENRR": "School enrollment, tertiary (% gross)",
    # Completion & Quality
    "SE.PRM.CMPT.ZS": "Primary completion rate (% of relevant age group)",
    "SE.XPD.TOTL.GD.ZS": "Government expenditure on education (% of GDP)",
    "SE.PRM.UNER": "Children out of school, primary",
}



# =============================================================================
# WORLD BANK DEMOGRAPHICS INDICATORS
# =============================================================================

WORLD_BANK_DEMOGRAPHICS_INDICATORS = {
    # Population
    "SP.POP.TOTL": "Population, total",
    "SP.POP.GROW": "Population growth (annual %)",
    # Urbanization
    "SP.URB.TOTL.IN.ZS": "Urban population (% of total)",
    "SP.URB.GROW": "Urban population growth (annual %)",
    # Fertility & Birth/Death
    "SP.DYN.TFRT.IN": "Fertility rate, total (births per woman)",
    "SP.DYN.CBRT.IN": "Birth rate, crude (per 1,000 people)",
    "SP.DYN.CDRT.IN": "Death rate, crude (per 1,000 people)",
    # Age Structure
    "SP.POP.DPND": "Age dependency ratio (% of working-age population)",
    "SP.POP.65UP.TO.ZS": "Population ages 65 and above (% of total)",
    "SP.POP.0014.TO.ZS": "Population ages 0-14 (% of total)",
    # Migration
    "SM.POP.NETM": "Net migration",
}



# =============================================================================
# WORLD BANK POVERTY & INEQUALITY INDICATORS
# =============================================================================

WORLD_BANK_POVERTY_INDICATORS = {
    # Poverty Rates
    "SI.POV.DDAY": "Poverty headcount ratio at $2.15/day (% of population)",
    "SI.POV.LMIC": "Poverty headcount ratio at $3.65/day (% of population)",
    "SI.POV.UMIC": "Poverty headcount ratio at $6.85/day (% of population)",
    # Inequality
    "SI.POV.GINI": "Gini index",
    "SI.DST.FRST.10": "Income share held by lowest 10%",
    "SI.DST.10TH.10": "Income share held by highest 10%",
}



# =============================================================================
# WORLD BANK ENVIRONMENT INDICATORS
# =============================================================================

WORLD_BANK_ENVIRONMENT_INDICATORS = {
    # CO2 & Emissions
    "EN.ATM.CO2E.PC": "CO2 emissions (metric tons per capita)",
    "EN.ATM.CO2E.KT": "CO2 emissions (kt)",
    # Energy
    "EG.USE.PCAP.KG.OE": "Energy use (kg of oil equivalent per capita)",
    "EG.FEC.RNEW.ZS": "Renewable energy consumption (% of total)",
    # Land & Forest
    "AG.LND.FRST.ZS": "Forest area (% of land area)",
    "AG.LND.AGRI.ZS": "Agricultural land (% of land area)",
    # Water & Air
    "ER.H2O.FWTL.ZS": "Freshwater withdrawals (% of internal resources)",
    "EN.ATM.PM25.MC.M3": "PM2.5 air pollution (micrograms per cubic meter)",
}



# =============================================================================
# WORLD BANK INFRASTRUCTURE INDICATORS
# =============================================================================

WORLD_BANK_INFRASTRUCTURE_INDICATORS = {
    # Internet & Mobile
    "IT.NET.USER.ZS": "Individuals using the Internet (% of population)",
    "IT.CEL.SETS.P2": "Mobile cellular subscriptions (per 100 people)",
    # Electricity
    "EG.ELC.ACCS.ZS": "Access to electricity (% of population)",
    "EG.USE.ELEC.KH.PC": "Electric power consumption (kWh per capita)",
    # Transport
    "IS.AIR.PSGR": "Air transport, passengers carried",
    "IS.RRS.TOTL.KM": "Rail lines (total route-km)",
}



# =============================================================================
# WORLD BANK GOVERNANCE INDICATORS
# =============================================================================

WORLD_BANK_GOVERNANCE_INDICATORS = {
    # World Governance Indicators
    "GE.EST": "Government Effectiveness (estimate)",
    "RQ.EST": "Regulatory Quality (estimate)",
    "RL.EST": "Rule of Law (estimate)",
    "CC.EST": "Control of Corruption (estimate)",
    "VA.EST": "Voice and Accountability (estimate)",
    "PV.EST": "Political Stability (estimate)",
    # Government Finance
    "GC.TAX.TOTL.GD.ZS": "Tax revenue (% of GDP)",
    "MS.MIL.XPND.GD.ZS": "Military expenditure (% of GDP)",
}



# =============================================================================
# WORLD BANK LABOR INDICATORS
# =============================================================================

WORLD_BANK_LABOR_INDICATORS = {
    # Labor Force Participation
    "SL.TLF.CACT.ZS": "Labor force participation rate (% of population 15+)",
    "SL.TLF.CACT.MA.ZS": "Labor force participation rate, male (% of males 15+)",
    "SL.TLF.CACT.FE.ZS": "Labor force participation rate, female (% of females 15+)",
    # Unemployment
    "SL.UEM.TOTL.ZS": "Unemployment, total (% of labor force)",
    "SL.UEM.1524.ZS": "Unemployment, youth (% of labor force ages 15-24)",
    # Employment by Sector
    "SL.AGR.EMPL.ZS": "Employment in agriculture (% of total employment)",
    "SL.IND.EMPL.ZS": "Employment in industry (% of total employment)",
    "SL.SRV.EMPL.ZS": "Employment in services (% of total employment)",
    # Vulnerability
    "SL.EMP.VULN.ZS": "Vulnerable employment (% of total employment)",
}



# =============================================================================
# WORLD BANK GENDER INDICATORS
# =============================================================================

WORLD_BANK_GENDER_INDICATORS = {
    "SG.GEN.PARL.ZS": "Women in parliament (% of seats)",
    "SL.TLF.CACT.FM.ZS": "Ratio of female to male labor participation (%)",
    "SE.ENR.PRIM.FM.ZS": "Ratio of female to male primary enrollment (%)",
    "SE.ENR.SECO.FM.ZS": "Ratio of female to male secondary enrollment (%)",
}



# =============================================================================
# WORLD BANK TRADE INDICATORS
# =============================================================================

WORLD_BANK_TRADE_INDICATORS = {
    "NE.EXP.GNFS.ZS": "Exports of goods and services (% of GDP)",
    "NE.IMP.GNFS.ZS": "Imports of goods and services (% of GDP)",
    "NE.TRD.GNFS.ZS": "Trade (% of GDP)",
    "TG.VAL.TOTL.GD.ZS": "Merchandise trade (% of GDP)",
    "BX.KLT.DINV.WD.GD.ZS": "Foreign direct investment, net inflows (% of GDP)",
    "BX.TRF.PWKR.CD.DT": "Personal remittances, received (current US$)",
}

# Grouped by category for easier access
INDICATOR_GROUPS = {
    "gdp": [
        "NY.GDP.MKTP.CD",
        "NY.GDP.MKTP.KD",
        "NY.GDP.MKTP.KD.ZG",
        "NY.GDP.PCAP.CD",
        "NY.GDP.PCAP.KD",
        "NY.GDP.PCAP.KD.ZG",
        "NY.GDP.PCAP.PP.CD",
    ],
    "trade": [
        "NE.EXP.GNFS.ZS",
        "NE.IMP.GNFS.ZS",
        "NE.TRD.GNFS.ZS",
        "BN.CAB.XOKA.CD",
        "BN.CAB.XOKA.GD.ZS",
    ],
    "inflation": [
        "FP.CPI.TOTL.ZG",
        "FP.CPI.TOTL",
    ],
    "investment": [
        "BX.KLT.DINV.CD.WD",
        "BX.KLT.DINV.WD.GD.ZS",
    ],
    "government": [
        "GC.REV.XGRT.GD.ZS",
        "GC.XPN.TOTL.GD.ZS",
        "GC.DOD.TOTL.GD.ZS",
        "GC.BAL.CASH.GD.ZS",
    ],
    "debt": [
        "DT.DOD.DECT.CD",
        "DT.DOD.DECT.GN.ZS",
    ],
    "labor": [
        "SL.UEM.TOTL.ZS",
        "SL.UEM.TOTL.NE.ZS",
    ],
    "population": [
        "SP.POP.TOTL",
        "SP.POP.GROW",
    ],
    "interest": [
        "FR.INR.RINR",
        "FR.INR.LEND",
    ],
    "health": [
        "SH.STA.SUIC.P5",
        "SH.STA.SUIC.MA.P5",
        "SH.STA.SUIC.FE.P5",
    ],
}


# =============================================================================
# UCDP INDICATORS (Conflict Data - Uppsala Conflict Data Program)
# =============================================================================

UCDP_INDICATORS = {
    # Battle-related deaths (state-based conflicts = wars)
    "UCDP.BD.TOTAL": "Battle-related deaths, total",
    "UCDP.BD.LOW": "Battle-related deaths, low estimate",
    "UCDP.BD.HIGH": "Battle-related deaths, high estimate",
    # Non-state conflict deaths (civil wars, rebel vs rebel)
    "UCDP.NS.TOTAL": "Non-state conflict deaths, total",
    "UCDP.NS.LOW": "Non-state conflict deaths, low estimate",
    "UCDP.NS.HIGH": "Non-state conflict deaths, high estimate",
    # One-sided violence (attacks on civilians)
    "UCDP.OS.TOTAL": "One-sided violence deaths, total",
    "UCDP.OS.LOW": "One-sided violence deaths, low estimate",
    "UCDP.OS.HIGH": "One-sided violence deaths, high estimate",
}

# UCDP API configuration
UCDP_API_BASE = "https://ucdpapi.pcr.uu.se/api"
UCDP_API_VERSION = "25.1"
