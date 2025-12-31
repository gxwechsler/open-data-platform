"""Admin - Database statistics and management."""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Admin | Open Data Platform", page_icon="⚙️", layout="wide")
st.title("⚙️ Admin Panel")

db = get_db_manager()

st.markdown("### Database Tables")

tables = db.execute_query("""
    SELECT table_name FROM information_schema.tables 
    WHERE table_schema = 'public' ORDER BY table_name
""")

if tables:
    for t in tables:
        table_name = t['table_name']
        count = db.execute_query(f"SELECT COUNT(*) as cnt FROM {table_name}")
        cnt = count[0]['cnt'] if count else 0
        st.metric(table_name, f"{cnt:,} records")

st.markdown("---")
st.markdown("### Time Series Data by Source")

source_stats = db.execute_query("""
    SELECT source, COUNT(*) as records,
           COUNT(DISTINCT indicator_code) as indicators,
           COUNT(DISTINCT country_iso3) as countries,
           MIN(year) as min_year, MAX(year) as max_year
    FROM time_series_unified_data
    GROUP BY source ORDER BY records DESC
""")

if source_stats:
    df = pd.DataFrame(source_stats)
    df.columns = ['Source', 'Records', 'Indicators', 'Countries', 'From', 'To']
    st.dataframe(df, use_container_width=True, hide_index=True)

st.markdown("---")
st.markdown("### Event Data Summary")

event_stats = db.execute_query("""
    SELECT disaster_type, COUNT(*) as events,
           SUM(deaths) as total_deaths,
           SUM(total_affected) as total_affected
    FROM event_level_unified_data
    GROUP BY disaster_type ORDER BY events DESC
""")

if event_stats:
    df = pd.DataFrame(event_stats)
    st.dataframe(df, use_container_width=True, hide_index=True)
