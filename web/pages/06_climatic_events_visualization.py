"""Climatic Events Visualization - Geographic view of disasters and crisis indicators."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Climatic Events Visualization | Open Data Platform", page_icon="🗺️", layout="wide")
st.title("🗺️ Climatic Events Visualization")

db = get_db_manager()

# --- Initialize logical state ---
if 'saved_cev_type' not in st.session_state:
    st.session_state.saved_cev_type = "All"
if 'saved_cev_yr' not in st.session_state:
    st.session_state.saved_cev_yr = None
if 'saved_cev_color' not in st.session_state:
    st.session_state.saved_cev_color = "deaths"

@st.cache_data(ttl=300)
def get_disaster_events():
    r = db.execute_query("SELECT * FROM event_level_unified_data ORDER BY year DESC")
    return pd.DataFrame(r) if r else pd.DataFrame()

@st.cache_data(ttl=300)
def get_crisis_data():
    r = db.execute_query("""
        SELECT country_iso3, country_name, year, indicator_code, indicator_name, value, units
        FROM time_series_unified_data 
        WHERE source IN ('LV', 'RR')
        ORDER BY year
    """)
    return pd.DataFrame(r) if r else pd.DataFrame()

tab1, tab2 = st.tabs(["🌪️ Disaster Events", "🏦 Crisis Indicators"])

with tab1:
    st.markdown("### Natural Disasters by Location")
    
    df = get_disaster_events()
    
    if df.empty:
        st.warning("No disaster event data found.")
    else:
        col1, col2, col3 = st.columns(3)
        
        with col1:
            types = ["All"] + sorted(df['disaster_type'].dropna().unique().tolist())
            try:
                type_idx = types.index(st.session_state.saved_cev_type)
            except ValueError:
                type_idx = 0
            sel_type = st.selectbox("Disaster Type", types, index=type_idx, key="widget_cev_type")
            st.session_state.saved_cev_type = sel_type
        
        with col2:
            mn, mx = int(df['year'].min()), int(df['year'].max())
            if st.session_state.saved_cev_yr is None:
                st.session_state.saved_cev_yr = (mn, mx)
            default_yr = (max(mn, st.session_state.saved_cev_yr[0]), min(mx, st.session_state.saved_cev_yr[1]))
            yr = st.slider("Years", mn, mx, default_yr, key="widget_cev_yr")
            st.session_state.saved_cev_yr = yr
        
        with col3:
            # CORRECT COLUMN: damage_usd (not total_damage)
            color_options = ["deaths", "total_affected", "damage_usd"]
            color_labels = {
                "deaths": "Deaths (persons)",
                "total_affected": "Affected (persons)", 
                "damage_usd": "Damage (USD)"
            }
            try:
                color_idx = color_options.index(st.session_state.saved_cev_color)
            except ValueError:
                color_idx = 0
            color = st.selectbox("Color By", color_options, index=color_idx,
                format_func=lambda x: color_labels.get(x, x), key="widget_cev_color")
            st.session_state.saved_cev_color = color
        
        # Apply filters
        df_map = df.copy()
        if sel_type != "All":
            df_map = df_map[df_map['disaster_type'] == sel_type]
        df_map = df_map[(df_map['year'] >= yr[0]) & (df_map['year'] <= yr[1])]
        
        # Filter out rows with missing coordinates
        df_map = df_map.dropna(subset=['latitude', 'longitude'])
        
        # Handle NaN in the color column - CRITICAL FIX
        if color in df_map.columns:
            # Remove rows where the color column is NaN (required for size parameter)
            df_map = df_map[df_map[color].notna()]
            # Also ensure positive values for size
            df_map = df_map[df_map[color] > 0]
        
        if df_map.empty:
            st.warning(f"No events with valid {color_labels.get(color, color)} data for the selected filters.")
        else:
            # Get unit for display
            unit_label = color_labels.get(color, color)
            
            st.markdown(f"**Disasters ({yr[0]}-{yr[1]})** | Color/Size: {unit_label}")
            
            # CORRECT COLUMN: country_iso3 (not country)
            fig = px.scatter_geo(df_map, 
                lat='latitude', 
                lon='longitude', 
                color=color,
                size=color,
                hover_name='event_name', 
                hover_data=['country_iso3', 'year', 'disaster_type', 'deaths', 'total_affected', 'damage_usd'],
                color_continuous_scale="Reds", 
                size_max=40, 
                title=f"Disasters by {unit_label}",
                labels={color: unit_label, 'country_iso3': 'Country'})
            
            fig.update_layout(
                geo=dict(showframe=False, showcoastlines=True, projection_type='natural earth'),
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Events", len(df_map))
            col2.metric("Total Deaths", f"{df_map['deaths'].sum():,.0f}" if df_map['deaths'].sum() > 0 else "N/A")
            col3.metric("Total Affected", f"{df_map['total_affected'].sum():,.0f}" if df_map['total_affected'].sum() > 0 else "N/A")

with tab2:
    st.markdown("### Crisis Indicators by Country")
    
    crisis_df = get_crisis_data()
    
    if crisis_df.empty:
        st.warning("No crisis indicator data found.")
    else:
        indicators = crisis_df[['indicator_code', 'indicator_name', 'units']].drop_duplicates()
        ind_opts = {r['indicator_code']: f"{r['indicator_name']}" for _, r in indicators.iterrows()}
        ind_units = {r['indicator_code']: r['units'] or 'Binary (0/1)' for _, r in indicators.iterrows()}
        
        col1, col2 = st.columns(2)
        
        with col1:
            sel_ind = st.selectbox("Indicator", list(ind_opts.keys()),
                format_func=lambda x: ind_opts.get(x, x), key="widget_cev_crisis_ind")
        
        with col2:
            mn, mx = int(crisis_df['year'].min()), int(crisis_df['year'].max())
            yr_crisis = st.slider("Year", mn, mx, mx, key="widget_cev_crisis_yr")
        
        # Filter data
        df_crisis = crisis_df[(crisis_df['indicator_code'] == sel_ind) & (crisis_df['year'] == yr_crisis)]
        
        if df_crisis.empty:
            st.warning(f"No data for {ind_opts.get(sel_ind)} in {yr_crisis}")
        else:
            unit_label = ind_units.get(sel_ind, 'Value')
            st.caption(f"**Units:** {unit_label}")
            
            fig = px.choropleth(df_crisis, 
                locations='country_iso3',
                color='value',
                hover_name='country_name',
                hover_data=['year', 'value'],
                title=f"{ind_opts.get(sel_ind, sel_ind)} ({yr_crisis})",
                color_continuous_scale="Reds",
                labels={'value': unit_label})
            
            fig.update_layout(
                geo=dict(showframe=False, projection_type='natural earth'),
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)
            
            # Summary stats
            if df_crisis['value'].isin([0, 1]).all():
                crisis_count = df_crisis[df_crisis['value'] == 1]['country_name'].nunique()
                st.metric(f"Countries in Crisis ({yr_crisis})", crisis_count)
            else:
                st.metric("Countries with Data", df_crisis['country_name'].nunique())

st.markdown("---")
st.caption("Sources: EM-DAT (disasters), Laeven-Valencia & Reinhart-Rogoff (crises)")
