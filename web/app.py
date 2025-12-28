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
    
    - **World Bank** - Development indicators for 44 countries (1970-2023)
    - **FRED** - Federal Reserve economic data (coming soon)
    - **IMF** - International Monetary Fund data (coming soon)
    - **OECD** - Economic statistics (coming soon)
    - **UNHCR** - Refugee data (coming soon)
    - **UCDP** - Armed conflict data (coming soon)
    - **UNESCO** - Education statistics (coming soon)
    - **IRENA** - Renewable energy data (coming soon)
    
    Use the sidebar to navigate between different analysis pages.
    """)

with col2:
    st.markdown("### 🔌 Data Status")
    if db.is_connected():
        st.success("✅ Database Connected")
        
        # Get stats from unified_indicators
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
                st.metric("Countries", stats['countries'])
                st.metric("Indicators", stats['indicators'])
                st.metric("Years", f"{stats['min_year']}-{stats['max_year']}")
        except:
            st.info("Loading stats...")
    else:
        st.error("❌ Database Not Connected")

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

# Categories overview
st.markdown("### 📁 Data Categories")

categories = [
    ("💰 Economy", "GDP, GNI, trade, exports, imports"),
    ("👷 Labor", "Unemployment, labor force, employment"),
    ("📈 Prices", "Inflation, CPI, deflators"),
    ("👥 Population", "Total, growth, urban, age structure"),
    ("🏚️ Poverty", "Poverty rates, Gini index, inequality"),
    ("🏥 Health", "Life expectancy, mortality, healthcare"),
    ("🎓 Education", "Literacy, enrollment, expenditure"),
    ("🏗️ Infrastructure", "Internet, electricity, transport"),
    ("🌍 Environment", "CO2 emissions, forest, renewable energy"),
    ("🏦 Finance", "Interest rates, credit, FDI, debt"),
]

cols = st.columns(5)
for i, (name, desc) in enumerate(categories):
    with cols[i % 5]:
        st.markdown(f"**{name}**")
        st.caption(desc)

st.markdown("---")
st.caption("**Data Sources:** World Bank, FRED, IMF, OECD, UNHCR, UCDP, UNESCO, UNSD, IRENA")
# Updated Sat Dec 27 21:25:55 PST 2025
