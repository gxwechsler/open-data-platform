"""Economic Crisis - Financial crisis indicators from LV and RR datasets."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Economic Crisis | Open Data Platform", page_icon="🏦", layout="wide")
st.title("🏦 Economic Crisis Database")
st.markdown("Crisis indicators from Laeven-Valencia (IMF) and Reinhart-Rogoff datasets")

db = get_db_manager()

# Session state
for key, val in [('ec_source', 'All'), ('ec_ind', None), ('ec_countries', None),
                 ('ec_yr_s', 1970), ('ec_yr_e', 2020)]:
    if key not in st.session_state:
        st.session_state[key] = val

@st.cache_data(ttl=300)
def get_indicators():
    r = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name, source
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
    q += " ORDER BY year"
    return pd.DataFrame(db.execute_query(q, p or None) or [])

# Sidebar
st.sidebar.header("Filters")

opts = ["All", "LV", "RR"]
idx = opts.index(st.session_state.ec_source) if st.session_state.ec_source in opts else 0
source = st.sidebar.selectbox("Source", opts, index=idx,
    format_func=lambda x: {"All": "All", "LV": "Laeven-Valencia", "RR": "Reinhart-Rogoff"}.get(x, x), key="ec_src")
st.session_state.ec_source = source

indicators = get_indicators()
if source != 'All':
    indicators = [i for i in indicators if i['source'] == source]
ind_opts = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in indicators}

if ind_opts:
    codes = list(ind_opts.keys())
    if st.session_state.ec_ind not in codes:
        st.session_state.ec_ind = codes[0]
    idx = codes.index(st.session_state.ec_ind)
    ind = st.sidebar.selectbox("Indicator", codes, index=idx, format_func=lambda x: ind_opts.get(x, x), key="ec_ind")
    st.session_state.ec_ind = ind
else:
    st.warning("No crisis indicators found.")
    st.stop()

countries = get_countries()
co_opts = {c['country_iso3']: c['country_name'] for c in countries}
if st.session_state.ec_countries is None:
    st.session_state.ec_countries = list(co_opts.keys())[:10]
valid = [c for c in st.session_state.ec_countries if c in co_opts] or list(co_opts.keys())[:10]
sel_cos = st.sidebar.multiselect("Countries", list(co_opts.keys()), default=valid,
    format_func=lambda x: co_opts.get(x, x), key="ec_co")
st.session_state.ec_countries = sel_cos

mn, mx = get_years()
yr = st.sidebar.slider("Years", mn, mx, (max(mn, st.session_state.ec_yr_s), min(mx, st.session_state.ec_yr_e)), key="ec_yr")
st.session_state.ec_yr_s, st.session_state.ec_yr_e = yr

# Stats
stats = get_stats()
if stats:
    st.markdown("### Data Coverage")
    stats_df = pd.DataFrame(stats)
    stats_df['source'] = stats_df['source'].map({'LV': 'Laeven-Valencia', 'RR': 'Reinhart-Rogoff'})
    stats_df.columns = ['Source', 'Records', 'Indicators', 'Countries', 'From', 'To']
    st.dataframe(stats_df, use_container_width=True, hide_index=True)

st.markdown("---")

if not sel_cos:
    st.warning("Select at least one country.")
    st.stop()

df = get_data(indicator=ind, countries=sel_cos, yr_s=yr[0], yr_e=yr[1], source=source)

if df.empty:
    st.warning("No data found for selected filters.")
    st.stop()

st.markdown(f"### {ind_opts.get(ind, ind)}")

tab1, tab2, tab3 = st.tabs(["📈 Timeline", "🗺️ Heatmap", "📋 Data"])

with tab1:
    # For binary indicators (0/1), show as scatter
    if df['value'].isin([0, 1]).all():
        crisis_df = df[df['value'] == 1]
        if not crisis_df.empty:
            fig = px.scatter(crisis_df, x='year', y='country_name', color='country_name',
                title="Crisis Events (value=1)", hover_data=['source'])
            fig.update_traces(marker=dict(size=12))
            fig.update_layout(xaxis_title="Year", yaxis_title="Country", showlegend=False, height=450)
            st.plotly_chart(fig, use_container_width=True)
            st.metric("Total Crisis-Years", len(crisis_df))
        else:
            st.success("No crisis events in selected range.")
    else:
        # Continuous indicator - line chart
        fig = px.line(df, x='year', y='value', color='country_name', markers=True)
        fig.update_layout(xaxis_title="Year", yaxis_title="Value", hovermode="x unified", height=450)
        st.plotly_chart(fig, use_container_width=True)

with tab2:
    pivot = df.pivot_table(index='country_name', columns='year', values='value', aggfunc='first')
    fig = px.imshow(pivot, color_continuous_scale="Reds", aspect="auto",
        title="Crisis Indicator Heatmap (1=crisis)")
    fig.update_layout(height=max(300, len(sel_cos) * 25))
    st.plotly_chart(fig, use_container_width=True)

with tab3:
    disp = df[['year', 'country_name', 'indicator_name', 'value', 'source']].sort_values(['country_name', 'year'])
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.download_button("📥 CSV", disp.to_csv(index=False), "crisis_data.csv", "text/csv")

st.markdown("---")
st.caption("Sources: Laeven-Valencia (IMF systemic banking crises), Reinhart-Rogoff (historical crises)")
