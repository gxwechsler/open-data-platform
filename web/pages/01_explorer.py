"""Explorer page - Browse available indicators and data."""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from database.connection import get_db_manager
from ingestion.fred_data import FREDData
from ingestion.emdat_disasters import DisasterData
from ingestion.reinhart_rogoff import CrisisData

st.set_page_config(page_title="Explorer | Open Data Platform", page_icon="🔍", layout="wide")
st.title("🔍 Data Explorer")

db = get_db_manager()
fred = FREDData(db)
disasters = DisasterData(db)
crises = CrisisData(db)

tab1, tab2, tab3 = st.tabs(["📈 Economic Indicators", "🌪️ Disasters", "🏦 Financial Crises"])

with tab1:
    st.markdown("### Federal Reserve Economic Data (FRED)")
    series_list = fred.get_available_series()
    df_series = pd.DataFrame(series_list)
    categories = ["All"] + fred.get_categories()
    selected_category = st.selectbox("Filter by Category", categories)
    if selected_category != "All":
        df_series = df_series[df_series['category'] == selected_category]
    st.dataframe(df_series[['series_id', 'name', 'category', 'units', 'frequency']], use_container_width=True, hide_index=True)

with tab2:
    st.markdown("### Natural Disasters (EM-DAT)")
    types_df = disasters.get_summary_by_type()
    if not types_df.empty:
        st.dataframe(types_df[['disaster_type', 'event_count', 'deaths']], use_container_width=True, hide_index=True)
    events_df = disasters.get_data()
    if not events_df.empty:
        st.markdown("#### Recent Major Events")
        st.dataframe(events_df[['year', 'country', 'disaster_type', 'event_name', 'deaths']].head(10), use_container_width=True, hide_index=True)

with tab3:
    st.markdown("### Financial Crises Database")
    summary_df = crises.get_summary_by_type()
    if not summary_df.empty:
        st.dataframe(summary_df, use_container_width=True, hide_index=True)
    crises_df = crises.get_data()
    if not crises_df.empty:
        st.markdown("#### Sample Crises")
        st.dataframe(crises_df[['start_year', 'country', 'crisis_type', 'description']].head(10), use_container_width=True, hide_index=True)
