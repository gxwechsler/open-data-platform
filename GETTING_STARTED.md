# Open Data Platform - Getting Started

A unified platform for querying socioeconomic, conflict, humanitarian, and energy data from multiple international sources.

## Quick Start
```bash
cd ~/open_data
source venv/bin/activate
opendata web
```

Then open **http://localhost:8501**

---

## Data Sources (5 Active)

| Source | Organization | Indicators | Records |
|--------|--------------|------------|---------|
| 🏦 WB | World Bank | 112 | 48,971 |
| 💰 IMF | International Monetary Fund | 12 | 11,536 |
| ⚡ IRENA | Int'l Renewable Energy Agency | 8 | 6,968 |
| 🏕️ UNHCR | UN Refugee Agency | 9 | 1,242 |
| ⚔️ UCDP | Uppsala Conflict Data Program | 9 | 1,157 |
| **Total** | | **150** | **69,874** |

---

## Categories (11)

- ECONOMIC - GDP, trade, inflation
- FINANCIAL - Debt, balance of payments
- HEALTH - Life expectancy, mortality
- EDUCATION - Enrollment, literacy
- DEMOGRAPHIC - Population, urbanization
- SOCIAL - Poverty, inequality
- ENVIRONMENT - Emissions, forests
- GOVERNANCE - Rule of law, corruption
- ENERGY - Renewables capacity/generation
- HUMANITARIAN - Refugees, displacement
- SECURITY - Conflict deaths, violence

---

## Key Indicators

### World Bank (112)
- NY.GDP.PCAP.CD - GDP per capita
- SP.DYN.LE00.IN - Life expectancy
- GB.XPD.RSDV.GD.ZS - R&D expenditure (% GDP)
- IP.PAT.RESD - Patent applications

### IMF (12)
- NGDP_RPCH - Real GDP growth
- PCPIPCH - Inflation rate
- GGXWDG_NGDP - Government debt (% GDP)

### IRENA (8)
- IRENA.CAP.SOLAR - Solar PV capacity (MW)
- IRENA.CAP.WIND - Wind capacity (MW)
- IRENA.GEN.RENEW - Renewable generation (GWh)

### UNHCR (9)
- UNHCR.REF.IN - Refugees hosted
- UNHCR.IDP - Internally displaced

### UCDP (9)
- UCDP.BD.TOTAL - Battle deaths
- UCDP.OS.TOTAL - One-sided violence

---

## Countries (42)

Americas: ARG, BRA, CHL, COL, MEX, USA, CAN
Europe: DEU, FRA, GBR, ITA, ESP, NLD, SWE, DNK, FIN, NOR, TUR
Asia: CHN, JPN, IND, VNM, SGP, ISR, IRN, ARE, SAU, QAT
Africa: ZAF, EGY, DZA, MAR, NGA, ETH, TZA, TUN, GHA, LBY, COD
Oceania: AUS, NZL

---

## CLI Commands
```bash
opendata ingest worldbank -s 2000 -e 2023
opendata ingest ucdp -s 1989 -e 2023
opendata query NY.GDP.PCAP.CD --countries USA,CHN
opendata catalog search "GDP"
opendata web
```

---

## Data Ingestion
```bash
# World Bank
opendata ingest worldbank -s 2000 -e 2023

# IMF
python3 -c "from open_data.ingestion.imf_datamapper import IMFDataMapperCollector; IMFDataMapperCollector(start_year=2000, end_year=2023).collect()"

# IRENA
python3 -c "from open_data.ingestion.irena import IRENACollector; IRENACollector(start_year=2000, end_year=2024).collect()"

# UNHCR
python3 -c "from open_data.ingestion.unhcr import UNHCRCollector; UNHCRCollector(start_year=2000, end_year=2023).collect()"

# UCDP
opendata ingest ucdp -s 1989 -e 2023
```

---

## Summary

| Metric | Value |
|--------|-------|
| Data Sources | 5 |
| Categories | 11 |
| Indicators | 150 |
| Countries | 42 |
| Observations | 69,874 |
| Years | 1989-2024 |
