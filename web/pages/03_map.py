"""Map page - Geographic visualization."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager
from ingestion.emdat_disasters import DisasterData
from ingestion.reinhart_rogoff import CrisisData

st.set_page_config(page_title="Map | Open Data Platform", page_icon="🗺️", layout="wide")
st.title("🗺️ Geographic Visualization")

db = get_db_manager()
disasters = DisasterData(db)
crises = CrisisData(db)

tab1, tab2 = st.tabs(["🌪️ Disaster Map", "🏦 Crisis Map"])

with tab1:
    st.markdown("### Natural Disasters by Location")
    col1, col2, col3 = st.columns(3)
    with col1:
        disaster_type = st.selectbox("Disaster Type", ["All"] + disasters.get_disaster_types())
    with col2:
        year_range = st.slider("Year Range", 2000, 2024, (2000, 2024))
    with col3:
        color_by = st.selectbox("Color By", ["deaths", "total_affected", "damage_usd"])
    
    df = disasters.get_data(disaster_type=disaster_type if disaster_type != "All" else None, year_start=year_range[0], year_end=year_range[1])
    
    if not df.empty and 'latitude' in df.columns:
        df_map = df.dropna(subset=['latitude', 'longitude'])
        if not df_map.empty:
            fig = px.scatter_geo(df_map, lat='latitude', lon='longitude', color=color_by, size=color_by,
                hover_name='event_name', color_continuous_scale="Reds", size_max=40,
                title=f"Natural Disasters ({year_range[0]}-{year_range[1]})")
            fig.update_layout(geo=dict(showframe=False, showcoastlines=True, projection_type='natural earth'), height=600)
            st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.markdown("### Financial Crises by Country")
    col1, col2 = st.columns(2)
    with col1:
        crisis_type = st.selectbox("Crisis Type", ["All"] + crises.get_crisis_types())
    with col2:
        crisis_year_range = st.slider("Year Range", 1900, 2024, (1980, 2024), key="crisis_years")
    
    df = crises.get_data(crisis_type=crisis_type if crisis_type != "All" else None, year_start=crisis_year_range[0], year_end=crisis_year_range[1])
    if not df.empty:
        country_counts = df.groupby('country_iso3').agg({'country': 'first', 'crisis_type': 'count'}).reset_index()
        country_counts = country_counts.rename(columns={'crisis_type': 'crisis_count'})
        fig = px.choropleth(country_counts, locations='country_iso3', color='crisis_count', hover_name='country',
            title=f"Financial Crises by Country ({crisis_year_range[0]}-{crisis_year_range[1]})", color_continuous_scale="Reds")
        fig.update_layout(geo=dict(showframe=False, showcoastlines=True, projection_type='natural earth'), height=600)
        st.plotly_chart(fig, use_container_width=True)
