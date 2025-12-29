"""World Bank - Global development indicators."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="World Bank | Open Data Platform", page_icon="🌍", layout="wide")
st.title("🌍 World Bank Data")

db = get_db_manager()

# Session state
for key, val in [('wb_ind', None), ('wb_countries', None), ('wb_yr_s', 1990), ('wb_yr_e', 2023)]:
    if key not in st.session_state:
        st.session_state[key] = val

@st.cache_data(ttl=300)
def get_indicators():
    r = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name, category
        FROM time_series_unified_data WHERE source = 'WB' ORDER BY indicator_name
    """)
    return r if r else []

@st.cache_data(ttl=300)
def get_countries():
    r = db.execute_query("""
        SELECT DISTINCT country_iso3, country_name
        FROM time_series_unified_data WHERE source = 'WB' ORDER BY country_name
    """)
    return r if r else []

@st.cache_data(ttl=300)
def get_years():
    r = db.execute_query("SELECT MIN(year) as mn, MAX(year) as mx FROM time_series_unified_data WHERE source = 'WB'")
    return (int(r[0]['mn']), int(r[0]['mx'])) if r and r[0]['mn'] else (1970, 2023)

def get_data(indicator, countries=None, yr_s=None, yr_e=None):
    q = "SELECT * FROM time_series_unified_data WHERE source = 'WB' AND indicator_code = :ind"
    p = {'ind': indicator}
    if countries:
        ph = ', '.join([f":c{i}" for i in range(len(countries))])
        q += f" AND country_iso3 IN ({ph})"
        for i, c in enumerate(countries):
            p[f'c{i}'] = c
    if yr_s:
        q += " AND year >= :ys"
        p['ys'] = yr_s
    if yr_e:
        q += " AND year <= :ye"
        p['ye'] = yr_e
    q += " ORDER BY year"
    return pd.DataFrame(db.execute_query(q, p) or [])

indicators = get_indicators()
countries = get_countries()

if not indicators:
    st.warning("No World Bank data found.")
    st.stop()

ind_opts = {i['indicator_code']: i['indicator_name'] for i in indicators}
co_opts = {c['country_iso3']: c['country_name'] for c in countries}

st.sidebar.header("Filters")

codes = list(ind_opts.keys())
if st.session_state.wb_ind not in codes:
    st.session_state.wb_ind = codes[0]
idx = codes.index(st.session_state.wb_ind)
ind = st.sidebar.selectbox("Indicator", codes, index=idx, format_func=lambda x: ind_opts.get(x, x), key="wb_ind")
st.session_state.wb_ind = ind

if st.session_state.wb_countries is None:
    st.session_state.wb_countries = list(co_opts.keys())[:5]
valid = [c for c in st.session_state.wb_countries if c in co_opts] or list(co_opts.keys())[:5]
sel_cos = st.sidebar.multiselect("Countries", list(co_opts.keys()), default=valid,
    format_func=lambda x: co_opts.get(x, x), key="wb_co")
st.session_state.wb_countries = sel_cos

mn, mx = get_years()
yr = st.sidebar.slider("Years", mn, mx,
    (max(mn, st.session_state.wb_yr_s), min(mx, st.session_state.wb_yr_e)), key="wb_yr")
st.session_state.wb_yr_s, st.session_state.wb_yr_e = yr

if not sel_cos:
    st.warning("Select at least one country.")
    st.stop()

df = get_data(ind, sel_cos, yr[0], yr[1])

if df.empty:
    st.warning("No data found.")
    st.stop()

st.markdown(f"### {ind_opts.get(ind, ind)}")

tab1, tab2, tab3 = st.tabs(["📈 Time Series", "🗺️ Map", "📋 Data"])

with tab1:
    fig = px.line(df, x='year', y='value', color='country_name', markers=True)
    fig.update_layout(xaxis_title="Year", yaxis_title="Value", hovermode="x unified", height=450)
    st.plotly_chart(fig, use_container_width=True)

with tab2:
    latest = df.sort_values('year').groupby('country_iso3').last().reset_index()
    if not latest.empty:
        fig = px.choropleth(latest, locations='country_iso3', color='value',
            hover_name='country_name', hover_data=['year'],
            title=f"{ind_opts.get(ind, ind)} (Latest)", color_continuous_scale="Viridis")
        fig.update_layout(geo=dict(showframe=False, projection_type='natural earth'), height=450)
        st.plotly_chart(fig, use_container_width=True)

with tab3:
    pivot = df.pivot_table(index='year', columns='country_name', values='value', aggfunc='first').reset_index()
    st.dataframe(pivot, use_container_width=True, hide_index=True)
    st.download_button("📥 CSV", df.to_csv(index=False), f"{ind}.csv", "text/csv")

st.markdown("---")
st.caption("Source: World Bank Development Indicators")
