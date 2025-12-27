"""Time Series page - Multi-indicator charts."""
import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager
from ingestion.fred_data import FREDData

st.set_page_config(page_title="Time Series | Open Data Platform", page_icon="📈", layout="wide")
st.title("📈 Time Series Analysis")

db = get_db_manager()
fred = FREDData(db)

st.sidebar.header("Chart Options")
series_list = fred.get_available_series()
series_options = {s['series_id']: f"{s['series_id']} - {s['name']}" for s in series_list}

selected_series = st.sidebar.multiselect("Select Indicators", options=list(series_options.keys()), default=["GDP", "UNRATE"], format_func=lambda x: series_options.get(x, x))

col1, col2 = st.sidebar.columns(2)
with col1:
    start_date = st.date_input("Start Date", value=date(2000, 1, 1))
with col2:
    end_date = st.date_input("End Date", value=date.today())

normalize = st.sidebar.checkbox("Normalize to 100", value=False)

if not selected_series:
    st.warning("Please select at least one indicator.")
else:
    dfs = []
    for series_id in selected_series:
        df = fred.get_series(series_id, start_date, end_date)
        if not df.empty:
            df['series_id'] = series_id
            info = next((s for s in series_list if s['series_id'] == series_id), {})
            df['series_name'] = info.get('name', series_id)
            dfs.append(df)
    
    if dfs:
        combined_df = pd.concat(dfs, ignore_index=True)
        if normalize:
            for series_id in combined_df['series_id'].unique():
                mask = combined_df['series_id'] == series_id
                first_value = combined_df.loc[mask, 'value'].iloc[0]
                if first_value != 0:
                    combined_df.loc[mask, 'value'] = (combined_df.loc[mask, 'value'] / first_value) * 100
        
        fig = px.line(combined_df, x='date', y='value', color='series_name', title="Economic Indicators Over Time")
        fig.update_layout(xaxis_title="Date", yaxis_title="Value" + (" (Normalized)" if normalize else ""), hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
        
        with st.expander("📋 View Data"):
            merged_df = fred.get_multiple_series(selected_series, start_date, end_date)
            st.dataframe(merged_df.sort_values('date', ascending=False), use_container_width=True, hide_index=True)
            csv = merged_df.to_csv(index=False)
            st.download_button("📥 Download CSV", data=csv, file_name="fred_data.csv", mime="text/csv")
