"""Data Explorer - Browse all indicators from all sources."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ingestion.unified_data import UnifiedData

st.set_page_config(page_title="Explorer | Open Data Platform", page_icon="🔍", layout="wide")
st.title("🔍 Data Explorer")

data = UnifiedData()

# Session state
for key, val in [('exp_source', 'All'), ('exp_category', 'All'), ('exp_indicator', 'All'),
                 ('exp_countries', None), ('exp_yr_s', 1990), ('exp_yr_e', 2024)]:
    if key not in st.session_state:
        st.session_state[key] = val

st.sidebar.header("Filters")

# Source
sources = data.get_sources()
opts = ["All"] + sources
idx = opts.index(st.session_state.exp_source) if st.session_state.exp_source in opts else 0
source = st.sidebar.selectbox("Source", opts, index=idx, key="exp_src_sel")
st.session_state.exp_source = source
src_filter = None if source == "All" else source

# Category
categories = data.get_categories(source=src_filter)
opts = ["All"] + categories
idx = opts.index(st.session_state.exp_category) if st.session_state.exp_category in opts else 0
category = st.sidebar.selectbox("Category", opts, index=idx, key="exp_cat_sel")
st.session_state.exp_category = category
cat_filter = None if category == "All" else category

# Indicators
indicators = data.get_indicators(source=src_filter, category=cat_filter)
ind_opts = {i['indicator_code']: i['indicator_name'] for i in indicators}
opts = ["All"] + list(ind_opts.keys())
idx = opts.index(st.session_state.exp_indicator) if st.session_state.exp_indicator in opts else 0
indicator = st.sidebar.selectbox("Indicator", opts, index=idx,
    format_func=lambda x: ind_opts.get(x, x) if x != "All" else "All", key="exp_ind_sel")
st.session_state.exp_indicator = indicator

# Search
search = st.sidebar.text_input("Search", placeholder="e.g., GDP, unemployment")
if search:
    indicators = data.search_indicators(search)
    ind_opts = {i['indicator_code']: i['indicator_name'] for i in indicators}

# Stats
stats = data.get_summary_stats()
col1, col2, col3, col4 = st.columns(4)
col1.metric("Records", f"{stats.get('total_records', 0):,}")
col2.metric("Sources", stats.get('sources', 0))
col3.metric("Countries", stats.get('countries', 0))
col4.metric("Indicators", len(indicators) if indicator == "All" else 1)

st.markdown("---")

if indicator != "All":
    ind_name = ind_opts.get(indicator, indicator)
    st.markdown(f"### {ind_name}")
    
    countries = data.get_countries(source=src_filter)
    country_opts = {c['country_iso3']: c['country_name'] for c in countries}
    
    if st.session_state.exp_countries is None:
        st.session_state.exp_countries = list(country_opts.keys())[:5]
    valid = [c for c in st.session_state.exp_countries if c in country_opts] or list(country_opts.keys())[:5]
    
    col1, col2 = st.columns(2)
    with col1:
        sel_countries = st.multiselect("Countries", list(country_opts.keys()), default=valid,
            format_func=lambda x: country_opts.get(x, x), key="exp_co_sel")
        st.session_state.exp_countries = sel_countries
    with col2:
        mn, mx = data.get_year_range(source=src_filter)
        yr = st.slider("Years", mn, mx, (max(mn, st.session_state.exp_yr_s), min(mx, st.session_state.exp_yr_e)), key="exp_yr")
        st.session_state.exp_yr_s, st.session_state.exp_yr_e = yr
    
    if sel_countries:
        df = data.get_data(indicator_code=indicator, countries=sel_countries, year_start=yr[0], year_end=yr[1])
        if not df.empty:
            fig = px.line(df, x='year', y='value', color='country_name', markers=True)
            fig.update_layout(xaxis_title="Year", yaxis_title="Value", hovermode="x unified", height=450)
            st.plotly_chart(fig, use_container_width=True)
            
            pivot = df.pivot_table(index='year', columns='country_name', values='value', aggfunc='first').reset_index()
            st.dataframe(pivot, use_container_width=True, hide_index=True)
            st.download_button("📥 CSV", df.to_csv(index=False), f"{indicator}.csv", "text/csv")
        else:
            st.warning("No data found.")
    else:
        st.info("Select at least one country.")
else:
    st.markdown(f"### Available Indicators ({len(indicators)})")
    if indicators:
        st.dataframe(pd.DataFrame(indicators)[['indicator_code', 'indicator_name', 'source', 'category']],
            use_container_width=True, hide_index=True)
        st.info("💡 Select an indicator from the dropdown to view data.")
    else:
        st.warning("No indicators found.")

st.markdown("---")
st.caption("Sources: World Bank, FRED, IMF, UNHCR, UCDP, IRENA, EM-DAT, Laeven-Valencia, Reinhart-Rogoff")
