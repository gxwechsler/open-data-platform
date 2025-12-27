"""Disasters page - Natural disaster data from EM-DAT."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager
from ingestion.emdat_disasters import DisasterData

st.set_page_config(page_title="Disasters | Open Data Platform", page_icon="🌪️", layout="wide")
st.title("🌪️ Natural Disasters Database")

db = get_db_manager()
disasters = DisasterData(db)

st.sidebar.header("Filters")
disaster_type = st.sidebar.selectbox("Disaster Type", ["All"] + disasters.get_disaster_types())
disaster_group = st.sidebar.selectbox("Disaster Group", ["All"] + disasters.get_disaster_groups())
year_range = st.sidebar.slider("Year Range", 2000, 2024, (2000, 2024))

countries = disasters.get_countries()
country_options = {c['country_iso3']: c['country'] for c in countries}
selected_country = st.sidebar.selectbox("Country", ["All"] + list(country_options.keys()), format_func=lambda x: country_options.get(x, x) if x != "All" else "All")

df = disasters.get_data(
    disaster_type=disaster_type if disaster_type != "All" else None,
    disaster_group=disaster_group if disaster_group != "All" else None,
    country=selected_country if selected_country != "All" else None,
    year_start=year_range[0], year_end=year_range[1]
)

if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Events", len(df))
    with col2:
        st.metric("Total Deaths", f"{df['deaths'].sum():,.0f}" if 'deaths' in df.columns else "N/A")
    with col3:
        st.metric("Total Affected", f"{df['total_affected'].sum():,.0f}" if 'total_affected' in df.columns else "N/A")
    with col4:
        damage = df['damage_usd'].sum() if 'damage_usd' in df.columns else 0
        st.metric("Total Damage", f"${damage/1e9:,.1f}B")
    
    tab1, tab2, tab3 = st.tabs(["📊 Charts", "📈 Trends", "📋 Data"])
    
    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            type_counts = df['disaster_type'].value_counts().reset_index()
            type_counts.columns = ['Type', 'Count']
            fig = px.pie(type_counts, values='Count', names='Type', title="Events by Type")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            if 'deaths' in df.columns:
                deaths_by_type = df.groupby('disaster_type')['deaths'].sum().reset_index().sort_values('deaths', ascending=True)
                fig = px.bar(deaths_by_type, x='deaths', y='disaster_type', orientation='h', title="Deaths by Type")
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Deadliest Events")
        if 'deaths' in df.columns:
            top = df.nlargest(10, 'deaths')[['year', 'country', 'disaster_type', 'event_name', 'deaths']]
            st.dataframe(top, use_container_width=True, hide_index=True)
    
    with tab2:
        yearly = disasters.get_summary_by_year(disaster_type=disaster_type if disaster_type != "All" else None)
        if not yearly.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(yearly, x='year', y='event_count', title="Events by Year")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.line(yearly, x='year', y='deaths', title="Deaths by Year")
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        display_cols = ['year', 'country', 'disaster_type', 'event_name', 'deaths', 'total_affected', 'damage_usd']
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols].sort_values('year', ascending=False), use_container_width=True, hide_index=True)
        csv = df.to_csv(index=False)
        st.download_button("📥 Download CSV", data=csv, file_name="disaster_data.csv", mime="text/csv")
else:
    st.warning("No disaster data found for the selected filters.")
