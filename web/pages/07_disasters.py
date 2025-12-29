"""Disasters page - Natural disaster data."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Disasters | Open Data Platform", page_icon="🌪️", layout="wide")
st.title("🌪️ Natural Disasters Database")

db = get_db_manager()

# Get filter options from database
@st.cache_data(ttl=300)
def get_disaster_types():
    result = db.execute_query("SELECT DISTINCT disaster_type FROM disasters WHERE disaster_type IS NOT NULL ORDER BY disaster_type")
    return [r['disaster_type'] for r in result] if result else []

@st.cache_data(ttl=300)
def get_disaster_groups():
    result = db.execute_query("SELECT DISTINCT disaster_group FROM disasters WHERE disaster_group IS NOT NULL ORDER BY disaster_group")
    return [r['disaster_group'] for r in result] if result else []

@st.cache_data(ttl=300)
def get_countries():
    result = db.execute_query("""
        SELECT DISTINCT d.country_iso3, c.name as country
        FROM disasters d
        LEFT JOIN countries c ON d.country_iso3 = c.iso3
        WHERE d.country_iso3 IS NOT NULL
        ORDER BY c.name
    """)
    return result if result else []

@st.cache_data(ttl=300)
def get_year_range():
    result = db.execute_query("SELECT MIN(year) as min_year, MAX(year) as max_year FROM disasters")
    if result and result[0]['min_year']:
        return int(result[0]['min_year']), int(result[0]['max_year'])
    return 1976, 2024

# Sidebar filters
st.sidebar.header("Filters")

disaster_types = get_disaster_types()
disaster_type = st.sidebar.selectbox("Disaster Type", ["All"] + disaster_types)

disaster_groups = get_disaster_groups()
disaster_group = st.sidebar.selectbox("Disaster Group", ["All"] + disaster_groups)

min_year, max_year = get_year_range()
year_range = st.sidebar.slider("Year Range", min_year, max_year, (min_year, max_year))

countries = get_countries()
country_options = {}
for c in countries:
    iso = c.get('country_iso3')
    name = c.get('country') or iso
    if iso:
        country_options[iso] = name

selected_country = st.sidebar.selectbox(
    "Country", 
    ["All"] + list(country_options.keys()), 
    format_func=lambda x: country_options.get(x, x) if x != "All" else "All"
)

# Build query
def get_disaster_data(disaster_type=None, disaster_group=None, country=None, year_start=None, year_end=None):
    query = """
        SELECT d.*, c.name as country
        FROM disasters d
        LEFT JOIN countries c ON d.country_iso3 = c.iso3
        WHERE 1=1
    """
    params = []
    
    if disaster_type:
        query += " AND d.disaster_type = %s"
        params.append(disaster_type)
    if disaster_group:
        query += " AND d.disaster_group = %s"
        params.append(disaster_group)
    if country:
        query += " AND d.country_iso3 = %s"
        params.append(country)
    if year_start:
        query += " AND d.year >= %s"
        params.append(year_start)
    if year_end:
        query += " AND d.year <= %s"
        params.append(year_end)
    
    query += " ORDER BY d.year DESC, d.deaths DESC NULLS LAST"
    
    result = db.execute_query(query, tuple(params) if params else None)
    return pd.DataFrame(result) if result else pd.DataFrame()

# Get data
df = get_disaster_data(
    disaster_type=disaster_type if disaster_type != "All" else None,
    disaster_group=disaster_group if disaster_group != "All" else None,
    country=selected_country if selected_country != "All" else None,
    year_start=year_range[0], 
    year_end=year_range[1]
)

if not df.empty:
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Events", len(df))
    with col2:
        deaths_total = df['deaths'].sum() if 'deaths' in df.columns else 0
        st.metric("Total Deaths", f"{int(deaths_total):,}" if pd.notna(deaths_total) else "N/A")
    with col3:
        affected_total = df['total_affected'].sum() if 'total_affected' in df.columns else 0
        st.metric("Total Affected", f"{int(affected_total):,}" if pd.notna(affected_total) else "N/A")
    with col4:
        damage = df['damage_usd'].sum() if 'damage_usd' in df.columns else 0
        if pd.notna(damage) and float(damage) > 0:
            st.metric("Total Damage", f"${float(damage)/1e9:,.1f}B")
        else:
            st.metric("Total Damage", "N/A")

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
                deaths_by_type = df.groupby('disaster_type')['deaths'].sum().reset_index()
                deaths_by_type = deaths_by_type.sort_values('deaths', ascending=True)
                fig = px.bar(deaths_by_type, x='deaths', y='disaster_type', orientation='h', title="Deaths by Type")
                st.plotly_chart(fig, use_container_width=True)

        st.markdown("#### Deadliest Events")
        if 'deaths' in df.columns:
            top_cols = ['year', 'country', 'disaster_type', 'event_name', 'deaths']
            available = [c for c in top_cols if c in df.columns]
            top = df.nlargest(10, 'deaths')[available]
            st.dataframe(top, use_container_width=True, hide_index=True)

    with tab2:
        # Yearly trends
        yearly = df.groupby('year').agg({
            'id': 'count',
            'deaths': 'sum',
            'total_affected': 'sum'
        }).reset_index()
        yearly.columns = ['year', 'event_count', 'deaths', 'affected']
        
        if not yearly.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(yearly, x='year', y='event_count', title="Events by Year")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.line(yearly, x='year', y='deaths', title="Deaths by Year", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            
            # Damage over time
            if 'damage_usd' in df.columns:
                damage_yearly = df.groupby('year')['damage_usd'].sum().reset_index()
                damage_yearly['damage_billions'] = damage_yearly['damage_usd'].astype(float) / 1e9
                fig = px.bar(damage_yearly, x='year', y='damage_billions', title="Economic Damage by Year ($B)")
                st.plotly_chart(fig, use_container_width=True)

    with tab3:
        display_cols = ['year', 'country', 'disaster_type', 'disaster_group', 'event_name', 'deaths', 'total_affected', 'damage_usd']
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols], use_container_width=True, hide_index=True)
        csv = df[available_cols].to_csv(index=False)
        st.download_button("📥 Download CSV", data=csv, file_name="disaster_data.csv", mime="text/csv")

else:
    st.warning("No disaster data found for the selected filters.")

# Footer
st.markdown("---")
st.caption("Data source: EM-DAT (CRED/UCLouvain) - Major historical disasters")
