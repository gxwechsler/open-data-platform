"""Economic Crisis - Financial crisis indicators from LV and RR datasets."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Economic Crisis | Open Data Platform", page_icon="🏦", layout="wide")
st.title("🏦 Economic Crisis Database")
st.markdown("Crisis indicators from Laeven-Valencia (IMF) and Reinhart-Rogoff datasets")

db = get_db_manager()

# --- Initialize logical state ---
if 'saved_ec_src' not in st.session_state:
    st.session_state.saved_ec_src = "All"
if 'saved_ec_ind' not in st.session_state:
    st.session_state.saved_ec_ind = None
if 'saved_ec_cos' not in st.session_state:
    st.session_state.saved_ec_cos = None
if 'saved_ec_yr' not in st.session_state:
    st.session_state.saved_ec_yr = None

@st.cache_data(ttl=300)
def get_indicators():
    r = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name, source, units, category
        FROM time_series_unified_data 
        WHERE source IN ('LV', 'RR') ORDER BY source, indicator_name
    """)
    return r if r else []

@st.cache_data(ttl=300)
def get_countries():
    r = db.execute_query("""
        SELECT DISTINCT country_iso3, country_name
        FROM time_series_unified_data 
        WHERE source IN ('LV', 'RR') ORDER BY country_name
    """)
    return r if r else []

@st.cache_data(ttl=300)
def get_years():
    r = db.execute_query("""
        SELECT MIN(year) as mn, MAX(year) as mx 
        FROM time_series_unified_data WHERE source IN ('LV', 'RR')
    """)
    return (int(r[0]['mn']), int(r[0]['mx'])) if r and r[0]['mn'] else (1800, 2020)

@st.cache_data(ttl=300)
def get_stats():
    r = db.execute_query("""
        SELECT source, COUNT(*) as records, COUNT(DISTINCT indicator_code) as indicators,
               COUNT(DISTINCT country_iso3) as countries, MIN(year) as min_yr, MAX(year) as max_yr
        FROM time_series_unified_data WHERE source IN ('LV', 'RR') GROUP BY source
    """)
    return r if r else []

def get_data(indicator=None, countries=None, yr_s=None, yr_e=None, source=None):
    q = "SELECT * FROM time_series_unified_data WHERE source IN ('LV', 'RR')"
    p = {}
    if indicator:
        q += " AND indicator_code = :ind"
        p['ind'] = indicator
    if source and source != 'All':
        q += " AND source = :src"
        p['src'] = source
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
    q += " ORDER BY country_name, year"
    return pd.DataFrame(db.execute_query(q, p or None) or [])

# --- Sidebar Filters ---
st.sidebar.header("Filters")

# Source
src_options = ["All", "LV", "RR"]
try:
    src_idx = src_options.index(st.session_state.saved_ec_src)
except ValueError:
    src_idx = 0

source = st.sidebar.selectbox("Source", src_options, index=src_idx,
    format_func=lambda x: {"All": "All Sources", "LV": "Laeven-Valencia (IMF)", "RR": "Reinhart-Rogoff"}.get(x, x),
    key="widget_ec_src")
st.session_state.saved_ec_src = source

# Indicator
indicators = get_indicators()
if source != 'All':
    indicators = [i for i in indicators if i['source'] == source]

ind_opts = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in indicators}
ind_units = {i['indicator_code']: i.get('units') or 'Binary (0/1)' for i in indicators}

if not ind_opts:
    st.warning("No crisis indicators found in database.")
    st.stop()

codes = list(ind_opts.keys())
if st.session_state.saved_ec_ind is None or st.session_state.saved_ec_ind not in codes:
    st.session_state.saved_ec_ind = codes[0]

try:
    ind_idx = codes.index(st.session_state.saved_ec_ind)
except ValueError:
    ind_idx = 0

ind = st.sidebar.selectbox("Indicator", codes, index=ind_idx,
    format_func=lambda x: ind_opts.get(x, x), key="widget_ec_ind")
st.session_state.saved_ec_ind = ind

# Countries
countries = get_countries()
co_opts = {c['country_iso3']: c['country_name'] for c in countries}
co_codes = list(co_opts.keys())

if st.session_state.saved_ec_cos is None:
    st.session_state.saved_ec_cos = co_codes  # Default: ALL countries
default_cos = [c for c in st.session_state.saved_ec_cos if c in co_codes] or co_codes

sel_cos = st.sidebar.multiselect("Countries", co_codes, default=default_cos,
    format_func=lambda x: co_opts.get(x, x), key="widget_ec_co")
st.session_state.saved_ec_cos = sel_cos

# Years
mn, mx = get_years()
if st.session_state.saved_ec_yr is None:
    st.session_state.saved_ec_yr = (mn, mx)  # Full range by default
default_yr = (max(mn, st.session_state.saved_ec_yr[0]), min(mx, st.session_state.saved_ec_yr[1]))

yr = st.sidebar.slider("Years", mn, mx, default_yr, key="widget_ec_yr")
st.session_state.saved_ec_yr = yr

# --- Stats ---
col1, col2 = st.columns(2)
with col1:
    stats = get_stats()
    if stats:
        st.markdown("### 📊 Data Coverage")
        stats_df = pd.DataFrame(stats)
        stats_df['source'] = stats_df['source'].map({'LV': 'Laeven-Valencia', 'RR': 'Reinhart-Rogoff'})
        stats_df.columns = ['Source', 'Records', 'Indicators', 'Countries', 'From', 'To']
        st.dataframe(stats_df, use_container_width=True, hide_index=True)

with col2:
    st.markdown("### 📋 Available Indicators")
    ind_df = pd.DataFrame(indicators)[['indicator_code', 'indicator_name', 'source']]
    ind_df.columns = ['Code', 'Name', 'Source']
    st.dataframe(ind_df, use_container_width=True, hide_index=True, height=200)

st.markdown("---")

if not sel_cos:
    st.warning("Select at least one country.")
    st.stop()

df = get_data(indicator=ind, countries=sel_cos, yr_s=yr[0], yr_e=yr[1], source=source)

if df.empty:
    st.warning(f"No data found for **{ind_opts.get(ind, ind)}** with selected filters.")
    st.info("Try selecting more countries or a wider year range.")
    st.stop()

unit = ind_units.get(ind, 'Value')
st.markdown(f"### {ind_opts.get(ind, ind)}")
st.caption(f"**Units:** {unit} | **Records:** {len(df):,}")

tab1, tab2, tab3 = st.tabs(["📈 Timeline", "🗺️ Heatmap", "📋 Data"])

with tab1:
    # Detect if binary indicator
    is_binary = df['value'].dropna().isin([0, 1]).all()
    
    if is_binary:
        crisis_df = df[df['value'] == 1]
        if not crisis_df.empty:
            fig = px.scatter(crisis_df, x='year', y='country_name', color='country_name',
                title=f"Crisis Events ({unit})", hover_data=['source'])
            fig.update_traces(marker=dict(size=12, symbol='square'))
            fig.update_layout(xaxis_title="Year", yaxis_title="Country", showlegend=False, height=max(400, len(crisis_df['country_name'].unique()) * 25))
            st.plotly_chart(fig, use_container_width=True)
            
            col1, col2, col3 = st.columns(3)
            col1.metric("Total Crisis-Years", len(crisis_df))
            col2.metric("Countries Affected", crisis_df['country_name'].nunique())
            col3.metric("Year Range", f"{crisis_df['year'].min()}-{crisis_df['year'].max()}")
        else:
            st.success("✅ No crisis events in selected range.")
    else:
        # Continuous indicator - line chart
        fig = px.line(df, x='year', y='value', color='country_name', markers=True,
            labels={'value': unit, 'year': 'Year', 'country_name': 'Country'})
        fig.update_layout(xaxis_title="Year", yaxis_title=unit, hovermode="x unified", height=450)
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    pivot = df.pivot_table(index='country_name', columns='year', values='value', aggfunc='first')
    if not pivot.empty:
        fig = px.imshow(pivot, color_continuous_scale="Reds", aspect="auto",
            title=f"Crisis Heatmap ({unit})",
            labels={'color': unit})
        fig.update_layout(height=max(300, len(pivot) * 20))
        st.plotly_chart(fig, use_container_width=True)
    else:
        st.info("No data to display in heatmap.")

with tab3:
    disp = df[['year', 'country_name', 'country_iso3', 'indicator_name', 'value', 'source']].sort_values(['country_name', 'year'])
    disp = disp.rename(columns={'value': f'Value ({unit})'})
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.download_button("📥 CSV", df.to_csv(index=False), "crisis_data.csv", "text/csv")

st.markdown("---")
st.caption("**Sources:** Laeven-Valencia (IMF systemic banking crises database), Reinhart-Rogoff (historical financial crises)")
