"""app: Open Data Platform - Home Page."""
import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.connection import get_db_manager

st.set_page_config(
    page_title="app: Open Data Platform",
    page_icon="📊",
    layout="wide"
)

st.title("📊 app: Open Data Platform")
st.markdown("### Global Development Indicators & Economic Data")

# Database status
db = get_db_manager()

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    Welcome to the **Open Data Platform**! This application provides access to:
    
    - ✅ **World Bank** - Development indicators (1970-2023)
    - ✅ **IMF** - Economic forecasts & fiscal data (1980-2030)
    - ✅ **FRED** - US Federal Reserve data (1950-2025)
    - ✅ **UNHCR** - Refugee & displacement data (2000-2024)
    - ✅ **IRENA** - Renewable energy statistics (2000-2024)
    - ✅ **UCDP** - Armed conflict data (1946-2024)
    - ✅ **EM-DAT** - Natural disaster data (1976-2024)
    - ✅ **Laeven-Valencia** - Banking crisis costs (1980-2008)
    - ✅ **Reinhart-Rogoff** - Historical crises (1340-2002)
    
    Use the sidebar to navigate between different analysis pages.
    """)

with col2:
    st.markdown("### 🔌 Data Status")
    if db.is_connected():
        st.success("✅ Database Connected")
        
        try:
            result = db.execute_query("""
                SELECT 
                    COUNT(*) as records,
                    COUNT(DISTINCT source) as sources,
                    COUNT(DISTINCT country_iso3) as countries,
                    COUNT(DISTINCT indicator_code) as indicators,
                    MIN(year) as min_year,
                    MAX(year) as max_year
                FROM unified_indicators
            """)
            if result:
                stats = result[0]
                st.metric("Total Records", f"{stats['records']:,}")
                st.metric("Sources", stats['sources'])
                st.metric("Countries", stats['countries'])
                st.metric("Indicators", stats['indicators'])
                st.metric("Years", f"{stats['min_year']}-{stats['max_year']}")
        except:
            st.info("Loading stats...")
    else:
        st.error("❌ Database Not Connected")

st.markdown("---")

# Source breakdown
st.markdown("### 📊 Data by Source")
try:
    result = db.execute_query("""
        SELECT source, COUNT(*) as records, 
               COUNT(DISTINCT indicator_code) as indicators,
               COUNT(DISTINCT country_iso3) as countries,
               MIN(year) as min_year, MAX(year) as max_year
        FROM unified_indicators 
        GROUP BY source 
        ORDER BY records DESC
    """)
    if result:
        import pandas as pd
        df = pd.DataFrame(result)
        df.columns = ['Source', 'Records', 'Indicators', 'Countries', 'From', 'To']
        source_names = {
            'WB': 'World Bank',
            'IMF': 'IMF',
            'FRED': 'FRED',
            'UNHCR': 'UNHCR',
            'IRENA': 'IRENA',
            'UCDP': 'UCDP',
            'EMDAT': 'EM-DAT',
            'LV': 'Laeven-Valencia',
            'RR': 'Reinhart-Rogoff'
        }
        df['Source'] = df['Source'].map(lambda x: source_names.get(x, x))
        st.dataframe(df, use_container_width=True, hide_index=True)
except:
    pass

st.markdown("---")

# Quick links
st.markdown("### 🚀 Quick Start")

col1, col2, col3 = st.columns(3)

with col1:
    st.markdown("""
    **📊 Data Explorer**
    
    Browse all available indicators by source and category.
    """)

with col2:
    st.markdown("""
    **📈 Time Series**
    
    Chart indicators over time and compare across countries.
    """)

with col3:
    st.markdown("""
    **🗺️ Map View**
    
    Visualize data geographically.
    """)

st.markdown("---")
st.caption("**Data Sources:** World Bank, IMF, FRED, UNHCR, IRENA, UCDP, EM-DAT, Laeven-Valencia, Reinhart-Rogoff")
# Updated Sun Dec 29 2025
