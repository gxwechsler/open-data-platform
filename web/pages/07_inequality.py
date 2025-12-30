"""Income & Wealth Inequality - World Inequality Database."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Inequality | Open Data Platform", page_icon="📊", layout="wide")
st.title("📊 Income & Wealth Inequality")
st.markdown("Distribution of income and wealth within and between countries")

db = get_db_manager()

@st.cache_data(ttl=300)
def get_wid_data():
    r = db.execute_query("""
        SELECT indicator_code, indicator_name, country_iso3, country_name, year, value, units
        FROM time_series_unified_data 
        WHERE source = 'WID' AND value IS NOT NULL
        ORDER BY country_name, year
    """)
    return pd.DataFrame(r) if r else pd.DataFrame()

df = get_wid_data()

if df.empty:
    st.error("No WID data found. Please run load_wid.py first.")
    st.stop()

df['is_global'] = df['indicator_code'].str.contains('GLOBAL')

# Sidebar filters
st.sidebar.header("Filters")

# Countries
country_list = df[df['country_iso3'] != 'WLD'][['country_iso3', 'country_name']].drop_duplicates().sort_values('country_name')
co_opts = dict(zip(country_list['country_iso3'], country_list['country_name']))
co_codes = list(co_opts.keys())

default_cos = [c for c in ['USA', 'FRA', 'CHN', 'BRA', 'ZAF'] if c in co_codes][:5]

sel_cos = st.sidebar.multiselect(
    "Countries", co_codes, default=default_cos,
    format_func=lambda x: co_opts.get(x, x)
)

# Indicator types
st.sidebar.markdown("---")
indicator_options = {
    'income_pretax': 'Pre-tax Income Shares',
    'income_posttax': 'Post-tax Income Shares', 
    'wealth': 'Wealth Shares',
    'gini': 'Gini Coefficients'
}
sel_indicators = st.sidebar.multiselect(
    "Indicator Types", list(indicator_options.keys()), 
    default=['income_pretax', 'wealth'],
    format_func=lambda x: indicator_options.get(x, x)
)

# Percentiles
percentile_options = {
    'p99p100': 'Top 1%',
    'p90p100': 'Top 10%',
    'p50p90': 'Middle 40%',
    'p0p50': 'Bottom 50%'
}
sel_percentiles = st.sidebar.multiselect(
    "Population Groups", list(percentile_options.keys()),
    default=['p99p100', 'p90p100', 'p0p50'],
    format_func=lambda x: percentile_options.get(x, x)
)

# Years
st.sidebar.markdown("---")
mn, mx = int(df['year'].min()), int(df['year'].max())
yr = st.sidebar.slider("Years", mn, mx, (1980, mx))

if not sel_cos:
    st.warning("Select at least one country.")
    st.stop()

filtered = df[(df['country_iso3'].isin(sel_cos)) & (df['year'] >= yr[0]) & (df['year'] <= yr[1])]
global_df = df[(df['is_global']) & (df['year'] >= yr[0]) & (df['year'] <= yr[1])]

tab1, tab2, tab3, tab4, tab5 = st.tabs(["📈 Income", "💰 Wealth", "🔄 Pre/Post Tax", "📊 Gini", "🌍 Global"])

with tab1:
    st.markdown("### Income Distribution")
    if 'income_pretax' in sel_indicators:
        for pct in sel_percentiles:
            data = filtered[filtered['indicator_code'].str.contains(f'sptincj.*{pct}', regex=True)]
            if not data.empty:
                fig = px.line(data, x='year', y='value', color='country_name',
                    title=f"{percentile_options.get(pct)} Pre-tax Income Share")
                fig.update_yaxes(tickformat='.0%')
                st.plotly_chart(fig, use_container_width=True)

with tab2:
    st.markdown("### Wealth Distribution")
    if 'wealth' in sel_indicators:
        for pct in sel_percentiles:
            data = filtered[filtered['indicator_code'].str.contains(f'shwealj.*{pct}', regex=True)]
            if not data.empty:
                fig = px.line(data, x='year', y='value', color='country_name',
                    title=f"{percentile_options.get(pct)} Wealth Share")
                fig.update_yaxes(tickformat='.0%')
                st.plotly_chart(fig, use_container_width=True)

with tab3:
    st.markdown("### Pre vs Post Tax")
    if 'income_pretax' in sel_indicators and 'income_posttax' in sel_indicators:
        for country in sel_cos[:3]:
            pre = filtered[(filtered['country_iso3']==country) & (filtered['indicator_code'].str.contains('sptincj.*p90p100', regex=True))].copy()
            post = filtered[(filtered['country_iso3']==country) & (filtered['indicator_code'].str.contains('sdiincj.*p90p100', regex=True))].copy()
            if not pre.empty and not post.empty:
                pre['type'] = 'Pre-tax'
                post['type'] = 'Post-tax'
                combined = pd.concat([pre, post])
                fig = px.line(combined, x='year', y='value', color='type',
                    title=f"{co_opts.get(country)}: Top 10% Income (Pre vs Post Tax)")
                fig.update_yaxes(tickformat='.0%')
                st.plotly_chart(fig, use_container_width=True)

with tab4:
    st.markdown("### Gini Coefficients")
    if 'gini' in sel_indicators:
        gini = filtered[filtered['indicator_code'].str.contains('gptincj', regex=True)]
        if not gini.empty:
            fig = px.line(gini, x='year', y='value', color='country_name', title="Pre-tax Income Gini")
            fig.update_yaxes(range=[0.3, 0.8])
            st.plotly_chart(fig, use_container_width=True)

with tab5:
    st.markdown("### Global Inequality")
    g_top10 = global_df[global_df['indicator_code'].str.contains('p90p100')]
    if not g_top10.empty:
        fig = px.line(g_top10, x='year', y='value', title="Global Top 10% Income Share")
        fig.update_yaxes(tickformat='.0%')
        st.plotly_chart(fig, use_container_width=True)
    g_bot50 = global_df[global_df['indicator_code'].str.contains('p0p50')]
    if not g_bot50.empty:
        fig = px.line(g_bot50, x='year', y='value', title="Global Bottom 50% Income Share")
        fig.update_yaxes(tickformat='.0%')
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.caption("**Source:** World Inequality Database (WID.world)")
