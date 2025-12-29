"""Map page - Geographic visualization."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Map | Open Data Platform", page_icon="🗺️", layout="wide")
st.title("🗺️ Geographic Visualization")

db = get_db_manager()

# Country names mapping
COUNTRY_NAMES = {
    "CHN": "China", "JPN": "Japan", "USA": "United States", "TUR": "Turkey",
    "IRN": "Iran", "IND": "India", "ITA": "Italy", "MEX": "Mexico",
    "CHL": "Chile", "NZL": "New Zealand", "COL": "Colombia", "DZA": "Algeria",
    "MAR": "Morocco", "DEU": "Germany", "GBR": "United Kingdom", "AUS": "Australia",
    "BRA": "Brazil", "NER": "Niger", "ETH": "Ethiopia", "GHA": "Ghana",
    "VNM": "Vietnam", "FRA": "France", "ESP": "Spain", "NLD": "Netherlands",
    "CAN": "Canada", "ZAF": "South Africa", "PRT": "Portugal", "COD": "DR Congo",
    "HTI": "Haiti", "ARG": "Argentina", "RUS": "Russia", "KOR": "South Korea",
    "IDN": "Indonesia", "THA": "Thailand", "MYS": "Malaysia", "PHL": "Philippines"
}

# Initialize session state
if 'map_disaster_type' not in st.session_state:
    st.session_state.map_disaster_type = "All"
if 'map_disaster_year_start' not in st.session_state:
    st.session_state.map_disaster_year_start = 1976
if 'map_disaster_year_end' not in st.session_state:
    st.session_state.map_disaster_year_end = 2024
if 'map_color_by' not in st.session_state:
    st.session_state.map_color_by = "deaths"
if 'map_crisis_type' not in st.session_state:
    st.session_state.map_crisis_type = "All"
if 'map_crisis_year_start' not in st.session_state:
    st.session_state.map_crisis_year_start = 1980
if 'map_crisis_year_end' not in st.session_state:
    st.session_state.map_crisis_year_end = 2024

# Helper functions
@st.cache_data(ttl=300)
def get_disaster_types():
    result = db.execute_query("SELECT DISTINCT disaster_type FROM disasters WHERE disaster_type IS NOT NULL ORDER BY disaster_type")
    return [r['disaster_type'] for r in result] if result else []

@st.cache_data(ttl=300)
def get_disaster_year_range():
    result = db.execute_query("SELECT MIN(year) as min_year, MAX(year) as max_year FROM disasters")
    if result and result[0]['min_year']:
        return int(result[0]['min_year']), int(result[0]['max_year'])
    return 1976, 2024

@st.cache_data(ttl=300)
def get_crisis_types():
    result = db.execute_query("SELECT DISTINCT crisis_type FROM financial_crises WHERE crisis_type IS NOT NULL ORDER BY crisis_type")
    return [r['crisis_type'] for r in result] if result else []

def get_disaster_data(disaster_type=None, year_start=None, year_end=None):
    query = "SELECT * FROM disasters WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    params = {}
    if disaster_type:
        query += " AND disaster_type = :dtype"
        params['dtype'] = disaster_type
    if year_start:
        query += " AND year >= :year_start"
        params['year_start'] = year_start
    if year_end:
        query += " AND year <= :year_end"
        params['year_end'] = year_end
    result = db.execute_query(query, params if params else None)
    if result:
        df = pd.DataFrame(result)
        df['country'] = df['country_iso3'].map(lambda x: COUNTRY_NAMES.get(x, x))
        return df
    return pd.DataFrame()

def get_crisis_data(crisis_type=None, year_start=None, year_end=None):
    query = "SELECT * FROM financial_crises WHERE 1=1"
    params = {}
    if crisis_type:
        query += " AND crisis_type = :ctype"
        params['ctype'] = crisis_type
    if year_start:
        query += " AND start_year >= :year_start"
        params['year_start'] = year_start
    if year_end:
        query += " AND start_year <= :year_end"
        params['year_end'] = year_end
    result = db.execute_query(query, params if params else None)
    if result:
        df = pd.DataFrame(result)
        df['country'] = df['country_iso3'].map(lambda x: COUNTRY_NAMES.get(x, x))
        return df
    return pd.DataFrame()

tab1, tab2 = st.tabs(["🌪️ Disaster Map", "🏦 Crisis Map"])

with tab1:
    st.markdown("### Natural Disasters by Location")
    
    disaster_types = get_disaster_types()
    d_min_year, d_max_year = get_disaster_year_range()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        dtype_options = ["All"] + disaster_types
        disaster_type = st.selectbox(
            "Disaster Type", 
            dtype_options,
            index=dtype_options.index(st.session_state.map_disaster_type) if st.session_state.map_disaster_type in dtype_options else 0,
            key="map_dtype_select"
        )
        st.session_state.map_disaster_type = disaster_type
    with col2:
        year_range = st.slider(
            "Year Range", 
            d_min_year, d_max_year, 
            (st.session_state.map_disaster_year_start, st.session_state.map_disaster_year_end),
            key="map_disaster_year_slider"
        )
        st.session_state.map_disaster_year_start = year_range[0]
        st.session_state.map_disaster_year_end = year_range[1]
    with col3:
        color_options = ["deaths", "total_affected", "damage_usd"]
        color_by = st.selectbox(
            "Color By", 
            color_options,
            index=color_options.index(st.session_state.map_color_by) if st.session_state.map_color_by in color_options else 0,
            key="map_color_select"
        )
        st.session_state.map_color_by = color_by
    
    df = get_disaster_data(
        disaster_type=disaster_type if disaster_type != "All" else None,
        year_start=year_range[0],
        year_end=year_range[1]
    )
    
    if not df.empty:
        df_map = df.dropna(subset=['latitude', 'longitude'])
        if not df_map.empty and color_by in df_map.columns:
            # Filter out rows with null/zero values for color column
            df_map = df_map[df_map[color_by].notna() & (df_map[color_by] > 0)]
            
            if not df_map.empty:
                fig = px.scatter_geo(
                    df_map, 
                    lat='latitude', 
                    lon='longitude', 
                    color=color_by, 
                    size=color_by,
                    hover_name='event_name',
                    hover_data=['country', 'year', 'disaster_type'],
                    color_continuous_scale="Reds",
                    size_max=40,
                    title=f"Natural Disasters ({year_range[0]}-{year_range[1]})"
                )
                fig.update_layout(
                    geo=dict(showframe=False, showcoastlines=True, projection_type='natural earth'),
                    height=600
                )
                st.plotly_chart(fig, use_container_width=True)
                
                st.metric("Events Shown", len(df_map))
            else:
                st.info("No disasters with valid location and damage data for selected filters.")
        else:
            st.info("No disasters with location data found.")
    else:
        st.warning("No disaster data found for selected filters.")

with tab2:
    st.markdown("### Financial Crises by Country")
    
    crisis_types = get_crisis_types()
    
    col1, col2 = st.columns(2)
    with col1:
        ctype_options = ["All"] + crisis_types
        crisis_type = st.selectbox(
            "Crisis Type", 
            ctype_options,
            index=ctype_options.index(st.session_state.map_crisis_type) if st.session_state.map_crisis_type in ctype_options else 0,
            key="map_ctype_select"
        )
        st.session_state.map_crisis_type = crisis_type
    with col2:
        crisis_year_range = st.slider(
            "Year Range", 
            1900, 2024, 
            (st.session_state.map_crisis_year_start, st.session_state.map_crisis_year_end),
            key="map_crisis_year_slider"
        )
        st.session_state.map_crisis_year_start = crisis_year_range[0]
        st.session_state.map_crisis_year_end = crisis_year_range[1]
    
    df = get_crisis_data(
        crisis_type=crisis_type if crisis_type != "All" else None,
        year_start=crisis_year_range[0],
        year_end=crisis_year_range[1]
    )
    
    if not df.empty:
        country_counts = df.groupby('country_iso3').agg({
            'country': 'first',
            'crisis_type': 'count'
        }).reset_index()
        country_counts = country_counts.rename(columns={'crisis_type': 'crisis_count'})
        
        fig = px.choropleth(
            country_counts,
            locations='country_iso3',
            color='crisis_count',
            hover_name='country',
            title=f"Financial Crises by Country ({crisis_year_range[0]}-{crisis_year_range[1]})",
            color_continuous_scale="Reds"
        )
        fig.update_layout(
            geo=dict(showframe=False, showcoastlines=True, projection_type='natural earth'),
            height=600
        )
        st.plotly_chart(fig, use_container_width=True)
        
        st.metric("Total Crises", len(df))
    else:
        st.warning("No crisis data found for selected filters.")
        st.info("💡 Crisis data is available in the **Economic Crisis** page and **Time Series** (filter by LV or RR sources)")

# Footer
st.markdown("---")
st.caption("Data sources: EM-DAT disasters, Laeven-Valencia & Reinhart-Rogoff crises")
