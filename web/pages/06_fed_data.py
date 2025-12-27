"""Fed Data page - Federal Reserve economic indicators."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from datetime import date
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager
from ingestion.fred_data import FREDData

st.set_page_config(page_title="Fed Data | Open Data Platform", page_icon="🏛️", layout="wide")
st.title("🏛️ Federal Reserve Economic Data")

db = get_db_manager()
fred = FREDData(db)

INDICATOR_GROUPS = {
    "Economic Output": ["GDP", "GDPC1"],
    "Labor Market": ["UNRATE", "PAYEMS"],
    "Interest Rates": ["FEDFUNDS", "DGS10"],
    "Money & Markets": ["M2SL", "SP500"],
}

st.sidebar.header("Options")
view_mode = st.sidebar.radio("View Mode", ["Dashboard", "Single Indicator", "Compare"])
date_range = st.sidebar.date_input("Date Range", value=(date(2000, 1, 1), date.today()))
start_date, end_date = (date_range[0], date_range[1]) if len(date_range) == 2 else (date(2000, 1, 1), date.today())

if view_mode == "Dashboard":
    st.markdown("### Economic Dashboard")
    for group_name, series_ids in INDICATOR_GROUPS.items():
        st.markdown(f"#### {group_name}")
        cols = st.columns(len(series_ids))
        for idx, series_id in enumerate(series_ids):
            with cols[idx]:
                df = fred.get_series(series_id, start_date, end_date)
                if not df.empty:
                    info = next((s for s in fred.get_available_series() if s['series_id'] == series_id), {})
                    latest = df['value'].iloc[-1]
                    if len(df) > 1:
                        pct_change = ((latest - df['value'].iloc[-2]) / df['value'].iloc[-2]) * 100
                    else:
                        pct_change = 0
                    st.metric(label=info.get('name', series_id)[:25], value=f"{latest:,.2f}", delta=f"{pct_change:+.2f}%")
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(x=df['date'], y=df['value'], mode='lines', fill='tozeroy'))
                    fig.update_layout(height=80, margin=dict(l=0, r=0, t=0, b=0), showlegend=False, xaxis=dict(visible=False), yaxis=dict(visible=False))
                    st.plotly_chart(fig, use_container_width=True)
        st.markdown("---")

elif view_mode == "Single Indicator":
    series_list = fred.get_available_series()
    series_options = {s['series_id']: f"{s['series_id']} - {s['name']}" for s in series_list}
    selected = st.selectbox("Select Indicator", list(series_options.keys()), format_func=lambda x: series_options.get(x, x))
    df = fred.get_series(selected, start_date, end_date)
    if not df.empty:
        info = next((s for s in series_list if s['series_id'] == selected), {})
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Latest", f"{df['value'].iloc[-1]:,.2f}")
        with col2:
            st.metric("Min", f"{df['value'].min():,.2f}")
        with col3:
            st.metric("Max", f"{df['value'].max():,.2f}")
        with col4:
            st.metric("Avg", f"{df['value'].mean():,.2f}")
        fig = px.line(df, x='date', y='value', title=info.get('name', selected))
        st.plotly_chart(fig, use_container_width=True)

else:  # Compare
    series_list = fred.get_available_series()
    series_options = {s['series_id']: f"{s['series_id']} - {s['name']}" for s in series_list}
    col1, col2 = st.columns(2)
    with col1:
        series1 = st.selectbox("First Indicator", list(series_options.keys()), format_func=lambda x: series_options.get(x, x), key="s1")
    with col2:
        series2 = st.selectbox("Second Indicator", list(series_options.keys()), index=1, format_func=lambda x: series_options.get(x, x), key="s2")
    normalize = st.checkbox("Normalize to 100", value=True)
    df1, df2 = fred.get_series(series1, start_date, end_date), fred.get_series(series2, start_date, end_date)
    if not df1.empty and not df2.empty:
        if normalize:
            df1['value'] = (df1['value'] / df1['value'].iloc[0]) * 100
            df2['value'] = (df2['value'] / df2['value'].iloc[0]) * 100
        info1, info2 = next((s for s in series_list if s['series_id'] == series1), {}), next((s for s in series_list if s['series_id'] == series2), {})
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df1['date'], y=df1['value'], name=info1.get('name', series1)[:25]))
        fig.add_trace(go.Scatter(x=df2['date'], y=df2['value'], name=info2.get('name', series2)[:25]))
        fig.update_layout(title="Indicator Comparison", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
