"""Acute Climatic Events - Natural disaster events database."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Acute Climatic Events | Open Data Platform", page_icon="🌪️", layout="wide")
st.title("🌪️ Acute Climatic Events")
st.markdown("Natural disaster events from EM-DAT database")

db = get_db_manager()

# --- Initialize logical state ---
if 'saved_ace_type' not in st.session_state:
    st.session_state.saved_ace_type = "All"
if 'saved_ace_group' not in st.session_state:
    st.session_state.saved_ace_group = "All"
if 'saved_ace_country' not in st.session_state:
    st.session_state.saved_ace_country = "All"
if 'saved_ace_yr' not in st.session_state:
    st.session_state.saved_ace_yr = None

@st.cache_data(ttl=300)
def get_events():
    r = db.execute_query("SELECT * FROM event_level_unified_data ORDER BY year DESC")
    return pd.DataFrame(r) if r else pd.DataFrame()

df = get_events()

if df.empty:
    st.warning("No event data found.")
    st.stop()

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Type
types = ["All"] + sorted(df['disaster_type'].dropna().unique().tolist())
try:
    type_idx = types.index(st.session_state.saved_ace_type)
except ValueError:
    type_idx = 0
sel_type = st.sidebar.selectbox("Type", types, index=type_idx, key="widget_ace_type")
st.session_state.saved_ace_type = sel_type

# Group
groups = ["All"] + sorted(df['disaster_group'].dropna().unique().tolist())
try:
    group_idx = groups.index(st.session_state.saved_ace_group)
except ValueError:
    group_idx = 0
sel_group = st.sidebar.selectbox("Group", groups, index=group_idx, key="widget_ace_group")
st.session_state.saved_ace_group = sel_group

# Country
countries_list = ["All"] + sorted(df['country'].dropna().unique().tolist())
try:
    country_idx = countries_list.index(st.session_state.saved_ace_country)
except ValueError:
    country_idx = 0
sel_country = st.sidebar.selectbox("Country", countries_list, index=country_idx, key="widget_ace_country")
st.session_state.saved_ace_country = sel_country

# Years
mn, mx = int(df['year'].min()), int(df['year'].max())
if st.session_state.saved_ace_yr is None:
    st.session_state.saved_ace_yr = (mn, mx)
default_yr = (max(mn, st.session_state.saved_ace_yr[0]), min(mx, st.session_state.saved_ace_yr[1]))

yr = st.sidebar.slider("Years", mn, mx, default_yr, key="widget_ace_yr")
st.session_state.saved_ace_yr = yr

# --- Apply filters ---
filtered = df.copy()
if sel_type != "All":
    filtered = filtered[filtered['disaster_type'] == sel_type]
if sel_group != "All":
    filtered = filtered[filtered['disaster_group'] == sel_group]
if sel_country != "All":
    filtered = filtered[filtered['country'] == sel_country]
filtered = filtered[(filtered['year'] >= yr[0]) & (filtered['year'] <= yr[1])]

if filtered.empty:
    st.warning("No events match filters.")
    st.stop()

# --- Metrics with units ---
st.markdown("### Summary Statistics")
col1, col2, col3, col4 = st.columns(4)
col1.metric("Events", f"{len(filtered):,}", help="Number of disaster events")
col2.metric("Deaths", f"{filtered['deaths'].sum():,.0f}" if filtered['deaths'].sum() > 0 else "N/A", help="Total deaths (persons)")
col3.metric("Affected", f"{filtered['total_affected'].sum():,.0f}" if filtered['total_affected'].sum() > 0 else "N/A", help="Total affected (persons)")
damage = filtered['total_damage'].sum()
col4.metric("Damage", f"${damage/1e9:.1f}B" if damage > 0 else "N/A", help="Total damage (USD)")

tab1, tab2, tab3 = st.tabs(["📊 Charts", "📈 Trends", "📋 Data"])

with tab1:
    col1, col2 = st.columns(2)
    with col1:
        type_counts = filtered['disaster_type'].value_counts()
        fig = px.pie(values=type_counts.values, names=type_counts.index, title="Events by Type")
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        deaths_by_type = filtered.groupby('disaster_type')['deaths'].sum().sort_values()
        fig = px.bar(x=deaths_by_type.values, y=deaths_by_type.index, orientation='h',
            title="Deaths by Type",
            labels={'x': 'Deaths (persons)', 'y': 'Event Type'})
        fig.update_layout(height=400, xaxis_title="Deaths (persons)")
        st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("### Deadliest Events")
    st.caption("**Units:** Deaths in persons")
    top = filtered.nlargest(10, 'deaths')[['year', 'country', 'disaster_type', 'event_name', 'deaths']]
    top['deaths'] = top['deaths'].apply(lambda x: f"{x:,.0f}" if pd.notna(x) else "N/A")
    st.dataframe(top, use_container_width=True, hide_index=True)

with tab2:
    yearly = filtered.groupby('year').agg({
        'event_name': 'count',
        'deaths': 'sum',
        'total_affected': 'sum',
        'total_damage': 'sum'
    }).reset_index()
    yearly.columns = ['Year', 'Events', 'Deaths', 'Affected', 'Damage']
    
    fig = px.bar(yearly, x='Year', y='Events', title="Events per Year",
        labels={'Events': 'Number of Events', 'Year': 'Year'})
    fig.update_layout(yaxis_title="Number of Events")
    st.plotly_chart(fig, use_container_width=True)
    
    col1, col2 = st.columns(2)
    with col1:
        fig = px.line(yearly, x='Year', y='Deaths', markers=True, title="Deaths per Year",
            labels={'Deaths': 'Deaths (persons)', 'Year': 'Year'})
        fig.update_layout(yaxis_title="Deaths (persons)")
        st.plotly_chart(fig, use_container_width=True)
    with col2:
        fig = px.line(yearly, x='Year', y='Affected', markers=True, title="People Affected per Year",
            labels={'Affected': 'Affected (persons)', 'Year': 'Year'})
        fig.update_layout(yaxis_title="Affected (persons)")
        st.plotly_chart(fig, use_container_width=True)
    
    # Damage trend
    fig = px.line(yearly, x='Year', y='Damage', markers=True, title="Economic Damage per Year",
        labels={'Damage': 'Damage (USD)', 'Year': 'Year'})
    fig.update_layout(yaxis_title="Damage (USD)")
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.markdown("### Event Data")
    st.caption("**Column Units:** Deaths (persons) | Affected (persons) | Damage (USD)")
    
    disp = filtered[['year', 'country', 'disaster_type', 'disaster_group', 'event_name', 
                     'deaths', 'total_affected', 'total_damage', 'latitude', 'longitude']].copy()
    disp = disp.rename(columns={
        'deaths': 'Deaths (persons)',
        'total_affected': 'Affected (persons)',
        'total_damage': 'Damage (USD)'
    })
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.download_button("📥 CSV", filtered.to_csv(index=False), "acute_climatic_events.csv", "text/csv")

st.markdown("---")
st.caption("**Source:** EM-DAT International Disaster Database | **Units:** Deaths & Affected in persons, Damage in USD")
