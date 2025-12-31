"""Open Data Platform - Home Page."""
import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Open Data Platform", page_icon="📊", layout="wide")

st.title("📊 app: Open Data Platform")
st.markdown("### Global Development Indicators & Economic Data")

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
- ✅ **WID** - Income & wealth inequality (1950-2024)

Use the sidebar to navigate between different analysis pages.
""")

st.markdown("---")

db = get_db_manager()

col1, col2 = st.columns(2)

with col1:
    st.markdown("### 🔌 Data Status")
    try:
        result = db.execute_query("SELECT 1")
        st.success("✅ Database Connected")
        
        stats = db.execute_query("""
            SELECT COUNT(*) as records, COUNT(DISTINCT source) as sources,
                   COUNT(DISTINCT country_iso3) as countries,
                   COUNT(DISTINCT indicator_code) as indicators,
                   MIN(year) as min_year, MAX(year) as max_year
            FROM time_series_unified_data
        """)
        if stats and stats[0]['records']:
            s = stats[0]
            st.metric("Total Records", f"{s['records']:,}")
            st.metric("Sources", s['sources'])
            st.metric("Countries", s['countries'])
            st.metric("Indicators", s['indicators'])
            st.metric("Year Range", f"{s['min_year']} - {s['max_year']}")
    except Exception as e:
        st.error(f"Database error: {e}")

with col2:
    st.markdown("### 📈 Data by Source")
    try:
        source_stats = db.execute_query("""
            SELECT source, COUNT(*) as records,
                   COUNT(DISTINCT indicator_code) as indicators,
                   COUNT(DISTINCT country_iso3) as countries,
                   MIN(year) as min_year, MAX(year) as max_year
            FROM time_series_unified_data
            GROUP BY source ORDER BY records DESC
        """)
        if source_stats:
            import pandas as pd
            df = pd.DataFrame(source_stats)
            df.columns = ['Source', 'Records', 'Indicators', 'Countries', 'From', 'To']
            st.dataframe(df, use_container_width=True, hide_index=True)
    except Exception as e:
        st.error(f"Query error: {e}")

st.markdown("---")

st.markdown("### 🚀 Quick Start")
st.markdown("""
1. **Explorer** - Browse and search all indicators
2. **Time Series** - Compare indicators across countries
3. **Map** - Geographic visualization of disasters and crises
4. **Economic Crisis** - Financial crisis analysis (LV/RR data)
5. **Fed Data** - US economic indicators from FRED
6. **Disasters** - Natural disaster events database
7. **World Bank** - Development indicators by country
""")

st.markdown("---")
st.caption("Built with Streamlit | Data from World Bank, IMF, FRED, UNHCR, UCDP, IRENA, EM-DAT, Laeven-Valencia, Reinhart-Rogoff")
