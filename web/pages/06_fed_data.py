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

st.set_page_config(page_title="Fed Data | Open Data Platform", page_icon="🏛️", layout="wide")
st.title("🏛️ Federal Reserve Economic Data")

db = get_db_manager()

# Initialize session state
if 'fed_view_mode' not in st.session_state:
    st.session_state.fed_view_mode = "Dashboard"
if 'fed_start_year' not in st.session_state:
    st.session_state.fed_start_year = 1990
if 'fed_end_year' not in st.session_state:
    st.session_state.fed_end_year = 2025
if 'fed_indicator' not in st.session_state:
    st.session_state.fed_indicator = None
if 'fed_indicator1' not in st.session_state:
    st.session_state.fed_indicator1 = None
if 'fed_indicator2' not in st.session_state:
    st.session_state.fed_indicator2 = None
if 'fed_normalize' not in st.session_state:
    st.session_state.fed_normalize = True

# Helper functions
@st.cache_data(ttl=300)
def get_fed_indicators():
    """Get available FRED indicators from unified_indicators."""
    result = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name, units
        FROM unified_indicators 
        WHERE source = 'FRED'
        ORDER BY indicator_name
    """)
    return result if result else []

@st.cache_data(ttl=300)
def get_fed_year_range():
    result = db.execute_query("""
        SELECT MIN(year) as min_year, MAX(year) as max_year 
        FROM unified_indicators 
        WHERE source = 'FRED'
    """)
    if result and result[0]['min_year']:
        return int(result[0]['min_year']), int(result[0]['max_year'])
    return 1950, 2025

def get_fed_data(indicator_code, year_start=None, year_end=None):
    query = """
        SELECT year, value, indicator_name, units
        FROM unified_indicators 
        WHERE source = 'FRED' AND indicator_code = :ind_code
    """
    params = {'ind_code': indicator_code}
    if year_start:
        query += " AND year >= :year_start"
        params['year_start'] = year_start
    if year_end:
        query += " AND year <= :year_end"
        params['year_end'] = year_end
    query += " ORDER BY year"
    
    result = db.execute_query(query, params)
    return pd.DataFrame(result) if result else pd.DataFrame()

# Get available indicators
indicators = get_fed_indicators()
if not indicators:
    st.warning("No FRED data found in the database.")
    st.info("💡 FRED data may be in the Time Series page. Try filtering by source 'FRED' there.")
    st.stop()

indicator_options = {i['indicator_code']: f"{i['indicator_code']} - {i['indicator_name']}" for i in indicators}
indicator_units = {i['indicator_code']: i.get('units', '') for i in indicators}

# Sidebar
st.sidebar.header("Options")

view_modes = ["Dashboard", "Single Indicator", "Compare"]
view_mode = st.sidebar.radio(
    "View Mode", 
    view_modes,
    index=view_modes.index(st.session_state.fed_view_mode) if st.session_state.fed_view_mode in view_modes else 0,
    key="fed_view_mode_radio"
)
st.session_state.fed_view_mode = view_mode

min_year, max_year = get_fed_year_range()
year_range = st.sidebar.slider(
    "Year Range",
    min_year, max_year,
    (max(min_year, st.session_state.fed_start_year), min(max_year, st.session_state.fed_end_year)),
    key="fed_year_slider"
)
st.session_state.fed_start_year = year_range[0]
st.session_state.fed_end_year = year_range[1]

if view_mode == "Dashboard":
    st.markdown("### Economic Dashboard (Annual Data)")
    
    # Group indicators
    indicator_groups = {
        "Output & Growth": ['FRED_GDP', 'FRED_GDPC1'],
        "Labor Market": ['FRED_UNRATE'],
        "Inflation": ['FRED_CPIAUCSL', 'FRED_PCEPI'],
        "Interest Rates": ['FRED_FEDFUNDS', 'FRED_DGS10', 'FRED_DGS2'],
        "Money Supply": ['FRED_M2SL'],
        "Housing": ['FRED_HOUST', 'FRED_CSUSHPINSA'],
        "Other": ['FRED_INDPRO', 'FRED_UMCSENT', 'FRED_BOPGSTB', 'FRED_GFDEBTN']
    }
    
    available_codes = set(indicator_options.keys())
    
    for group_name, codes in indicator_groups.items():
        group_indicators = [c for c in codes if c in available_codes]
        if not group_indicators:
            continue
            
        st.markdown(f"#### {group_name}")
        cols = st.columns(min(len(group_indicators), 4))
        
        for idx, ind_code in enumerate(group_indicators):
            with cols[idx % 4]:
                df = get_fed_data(ind_code, year_range[0], year_range[1])
                if not df.empty:
                    ind_name = indicator_options.get(ind_code, ind_code).split(' - ')[-1][:20]
                    latest = df['value'].iloc[-1]
                    latest_year = df['year'].iloc[-1]
                    
                    # Calculate change
                    if len(df) > 1:
                        prev = df['value'].iloc[-2]
                        if prev and prev != 0:
                            pct_change = ((latest - prev) / abs(prev)) * 100
                        else:
                            pct_change = 0
                    else:
                        pct_change = 0
                    
                    # Format value
                    if abs(latest) >= 1e9:
                        val_str = f"${latest/1e9:.1f}B"
                    elif abs(latest) >= 1e6:
                        val_str = f"{latest/1e6:.1f}M"
                    elif abs(latest) >= 1000:
                        val_str = f"{latest:,.0f}"
                    else:
                        val_str = f"{latest:.2f}"
                    
                    st.metric(
                        label=f"{ind_name} ({latest_year})",
                        value=val_str,
                        delta=f"{pct_change:+.1f}%"
                    )
                    
                    # Mini chart
                    fig = go.Figure()
                    fig.add_trace(go.Scatter(
                        x=df['year'], 
                        y=df['value'], 
                        mode='lines', 
                        fill='tozeroy',
                        line=dict(color='#1f77b4')
                    ))
                    fig.update_layout(
                        height=80,
                        margin=dict(l=0, r=0, t=0, b=0),
                        showlegend=False,
                        xaxis=dict(visible=False),
                        yaxis=dict(visible=False)
                    )
                    st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("---")

elif view_mode == "Single Indicator":
    # Set default indicator
    if st.session_state.fed_indicator is None or st.session_state.fed_indicator not in indicator_options:
        st.session_state.fed_indicator = list(indicator_options.keys())[0]
    
    selected = st.selectbox(
        "Select Indicator",
        list(indicator_options.keys()),
        index=list(indicator_options.keys()).index(st.session_state.fed_indicator) if st.session_state.fed_indicator in indicator_options else 0,
        format_func=lambda x: indicator_options.get(x, x),
        key="fed_single_select"
    )
    st.session_state.fed_indicator = selected
    
    df = get_fed_data(selected, year_range[0], year_range[1])
    
    if not df.empty:
        ind_name = indicator_options.get(selected, selected).split(' - ')[-1]
        units = indicator_units.get(selected, '')
        
        col1, col2, col3, col4 = st.columns(4)
        with col1:
            st.metric("Latest", f"{df['value'].iloc[-1]:,.2f}")
        with col2:
            st.metric("Min", f"{df['value'].min():,.2f}")
        with col3:
            st.metric("Max", f"{df['value'].max():,.2f}")
        with col4:
            st.metric("Avg", f"{df['value'].mean():,.2f}")
        
        fig = px.line(df, x='year', y='value', title=ind_name, markers=True)
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title=units if units else "Value",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Data table
        st.markdown("### Data")
        st.dataframe(df[['year', 'value']], use_container_width=True, hide_index=True)
    else:
        st.warning("No data found for selected indicator.")

else:  # Compare
    # Set defaults
    indicator_list = list(indicator_options.keys())
    if st.session_state.fed_indicator1 is None or st.session_state.fed_indicator1 not in indicator_options:
        st.session_state.fed_indicator1 = indicator_list[0]
    if st.session_state.fed_indicator2 is None or st.session_state.fed_indicator2 not in indicator_options:
        st.session_state.fed_indicator2 = indicator_list[1] if len(indicator_list) > 1 else indicator_list[0]
    
    col1, col2 = st.columns(2)
    with col1:
        series1 = st.selectbox(
            "First Indicator",
            indicator_list,
            index=indicator_list.index(st.session_state.fed_indicator1) if st.session_state.fed_indicator1 in indicator_list else 0,
            format_func=lambda x: indicator_options.get(x, x),
            key="fed_compare1"
        )
        st.session_state.fed_indicator1 = series1
    with col2:
        series2 = st.selectbox(
            "Second Indicator",
            indicator_list,
            index=indicator_list.index(st.session_state.fed_indicator2) if st.session_state.fed_indicator2 in indicator_list else 0,
            format_func=lambda x: indicator_options.get(x, x),
            key="fed_compare2"
        )
        st.session_state.fed_indicator2 = series2
    
    normalize = st.checkbox(
        "Normalize to 100",
        value=st.session_state.fed_normalize,
        key="fed_normalize_cb"
    )
    st.session_state.fed_normalize = normalize
    
    df1 = get_fed_data(series1, year_range[0], year_range[1])
    df2 = get_fed_data(series2, year_range[0], year_range[1])
    
    if not df1.empty and not df2.empty:
        if normalize:
            if df1['value'].iloc[0] != 0:
                df1['value'] = (df1['value'] / df1['value'].iloc[0]) * 100
            if df2['value'].iloc[0] != 0:
                df2['value'] = (df2['value'] / df2['value'].iloc[0]) * 100
        
        name1 = indicator_options.get(series1, series1).split(' - ')[-1][:25]
        name2 = indicator_options.get(series2, series2).split(' - ')[-1][:25]
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df1['year'], y=df1['value'], name=name1, mode='lines+markers'))
        fig.add_trace(go.Scatter(x=df2['year'], y=df2['value'], name=name2, mode='lines+markers'))
        fig.update_layout(
            title="Indicator Comparison",
            xaxis_title="Year",
            yaxis_title="Index (Base=100)" if normalize else "Value",
            hovermode="x unified"
        )
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.warning("No data found for one or both indicators.")

# Footer
st.markdown("---")
st.caption("**Data Source:** Federal Reserve Economic Data (FRED) via St. Louis Fed - Annual averages")
