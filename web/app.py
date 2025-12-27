"""Open Data Platform - Main Streamlit Application."""
import streamlit as st
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from database.connection import get_db_manager

st.set_page_config(page_title="Open Data Platform", page_icon="📊", layout="wide", initial_sidebar_state="expanded")

@st.cache_resource
def init_db():
    return get_db_manager()

db = init_db()

st.title("📊 Open Data Platform")
st.markdown("### Economic, Financial Crisis, and Disaster Data Visualization")

col1, col2 = st.columns([2, 1])

with col1:
    st.markdown("""
    Welcome to the Open Data Platform! This application provides access to:
    
    - **Economic Indicators** - Federal Reserve (FRED) time series data
    - **Financial Crises** - 800+ years of banking, currency, and sovereign debt crises
    - **Natural Disasters** - Global disaster data from EM-DAT
    
    Use the sidebar to navigate between different analysis pages.
    """)

with col2:
    st.markdown("#### 📡 Data Status")
    if db.is_connected():
        st.success("✅ Database Connected")
        for table, label in {'fed_series': 'FRED Data Points', 'disasters': 'Disaster Events', 'financial_crises': 'Financial Crises'}.items():
            try:
                st.metric(label, f"{db.get_table_count(table):,}")
            except:
                st.metric(label, "N/A")
    else:
        st.warning("⚠️ Using Sample Data")
        st.caption("Database not connected. Using built-in sample data.")

st.markdown("---")
st.markdown("### 📈 Quick Overview")
col1, col2, col3, col4 = st.columns(4)
with col1:
    st.metric(label="📊 FRED Series", value="10+")
with col2:
    st.metric(label="🏦 Crisis Types", value="6")
with col3:
    st.metric(label="🌪️ Disaster Types", value="12")
with col4:
    st.metric(label="🌍 Countries", value="100+")

st.markdown("---")
st.caption("**Data Sources:** FRED, EM-DAT, Reinhart-Rogoff, Laeven-Valencia")
