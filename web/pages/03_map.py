"""Map - Geographic visualization."""
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

COUNTRY_NAMES = {
    "CHN": "China", "JPN": "Japan", "USA": "United States", "TUR": "Turkey",
    "IRN": "Iran", "IND": "India", "ITA": "Italy", "MEX": "Mexico", "CHL": "Chile",
    "NZL": "New Zealand", "COL": "Colombia", "DZA": "Algeria", "MAR": "Morocco",
    "DEU": "Germany", "GBR": "United Kingdom", "AUS": "Australia", "BRA": "Brazil",
    "FRA": "France", "ESP": "Spain", "ARG": "Argentina", "RUS": "Russia",
    "KOR": "South Korea", "IDN": "Indonesia", "THA": "Thailand", "GRC": "Greece",
    "PRT": "Portugal", "PAK": "Pakistan", "BGD": "Bangladesh", "PHL": "Philippines"
}

# Session state
for key, val in [('map_dtype', 'All'), ('map_dyr_s', 1976), ('map_dyr_e', 2024),
                 ('map_color', 'deaths'), ('map_cind', None), ('map_cyr', 2020)]:
    if key not in st.session_state:
        st.session_state[key] = val

@st.cache_data(ttl=300)
def get_disaster_types():
    r = db.execute_query("SELECT DISTINCT disaster_type FROM event_level_unified_data WHERE disaster_type IS NOT NULL ORDER BY disaster_type")
    return [x['disaster_type'] for x in r] if r else []

@st.cache_data(ttl=300)
def get_disaster_years():
    r = db.execute_query("SELECT MIN(year) as mn, MAX(year) as mx FROM event_level_unified_data")
    return (int(r[0]['mn']), int(r[0]['mx'])) if r and r[0]['mn'] else (1976, 2024)

@st.cache_data(ttl=300)
def get_crisis_indicators():
    r = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name 
        FROM time_series_unified_data 
        WHERE source IN ('LV', 'RR') ORDER BY indicator_name
    """)
    return r if r else []

def get_disasters(dtype=None, yr_s=None, yr_e=None):
    q = "SELECT * FROM event_level_unified_data WHERE latitude IS NOT NULL AND longitude IS NOT NULL"
    p = {}
    if dtype:
        q += " AND disaster_type = :dt"
        p['dt'] = dtype
    if yr_s:
        q += " AND year >= :ys"
        p['ys'] = yr_s
    if yr_e:
        q += " AND year <= :ye"
        p['ye'] = yr_e
    r = db.execute_query(q, p or None)
    if r:
        df = pd.DataFrame(r)
        df['country'] = df['country_iso3'].map(lambda x: COUNTRY_NAMES.get(x, x))
        return df
    return pd.DataFrame()

def get_crisis_map_data(indicator, year):
    q = """
        SELECT country_iso3, country_name, value 
        FROM time_series_unified_data 
        WHERE indicator_code = :ind AND year = :yr
    """
    r = db.execute_query(q, {'ind': indicator, 'yr': year})
    return pd.DataFrame(r) if r else pd.DataFrame()

tab1, tab2 = st.tabs(["🌪️ Disaster Events", "🏦 Crisis Indicators"])

with tab1:
    st.markdown("### Natural Disasters by Location")
    dtypes = get_disaster_types()
    d_min, d_max = get_disaster_years()
    
    col1, col2, col3 = st.columns(3)
    with col1:
        opts = ["All"] + dtypes
        idx = opts.index(st.session_state.map_dtype) if st.session_state.map_dtype in opts else 0
        dtype = st.selectbox("Disaster Type", opts, index=idx, key="map_dt")
        st.session_state.map_dtype = dtype
    with col2:
        yr = st.slider("Years", d_min, d_max, (st.session_state.map_dyr_s, st.session_state.map_dyr_e), key="map_dyr")
        st.session_state.map_dyr_s, st.session_state.map_dyr_e = yr
    with col3:
        copts = ["deaths", "total_affected", "damage_usd"]
        idx = copts.index(st.session_state.map_color) if st.session_state.map_color in copts else 0
        color = st.selectbox("Color By", copts, index=idx, key="map_col")
        st.session_state.map_color = color
    
    df = get_disasters(dtype if dtype != "All" else None, yr[0], yr[1])
    if not df.empty:
        df_map = df.dropna(subset=['latitude', 'longitude'])
        if color in df_map.columns:
            df_map = df_map[df_map[color].notna() & (df_map[color] > 0)]
        if not df_map.empty:
            fig = px.scatter_geo(df_map, lat='latitude', lon='longitude', color=color, size=color,
                hover_name='event_name', hover_data=['country', 'year', 'disaster_type'],
                color_continuous_scale="Reds", size_max=40, title=f"Disasters ({yr[0]}-{yr[1]})")
            fig.update_layout(geo=dict(showframe=False, showcoastlines=True, projection_type='natural earth'), height=550)
            st.plotly_chart(fig, use_container_width=True)
            st.metric("Events", len(df_map))
        else:
            st.info("No disasters with valid coordinates for selected filters.")
    else:
        st.warning("No disaster data found.")

with tab2:
    st.markdown("### Crisis Indicators by Country")
    st.caption("Binary indicators (1=crisis) from Laeven-Valencia and Reinhart-Rogoff datasets")
    
    indicators = get_crisis_indicators()
    if indicators:
        ind_opts = {i['indicator_code']: i['indicator_name'] for i in indicators}
        codes = list(ind_opts.keys())
        
        col1, col2 = st.columns(2)
        with col1:
            if st.session_state.map_cind not in codes:
                st.session_state.map_cind = codes[0]
            idx = codes.index(st.session_state.map_cind)
            ind = st.selectbox("Indicator", codes, index=idx, format_func=lambda x: ind_opts.get(x, x), key="map_cind_sel")
            st.session_state.map_cind = ind
        with col2:
            year = st.slider("Year", 1970, 2020, st.session_state.map_cyr, key="map_cyr_sl")
            st.session_state.map_cyr = year
        
        df = get_crisis_map_data(ind, year)
        if not df.empty:
            fig = px.choropleth(df, locations='country_iso3', color='value', hover_name='country_name',
                title=f"{ind_opts.get(ind, ind)} ({year})", color_continuous_scale="Reds")
            fig.update_layout(geo=dict(showframe=False, projection_type='natural earth'), height=550)
            st.plotly_chart(fig, use_container_width=True)
            
            crisis_countries = df[df['value'] == 1]['country_name'].tolist()
            if crisis_countries:
                st.warning(f"Countries in crisis: {', '.join(crisis_countries)}")
            else:
                st.success("No countries in crisis for this indicator/year.")
        else:
            st.info("No data for selected indicator/year.")
    else:
        st.warning("No crisis indicators found.")

st.markdown("---")
st.caption("Data: EM-DAT disasters, Laeven-Valencia & Reinhart-Rogoff crisis indicators")
