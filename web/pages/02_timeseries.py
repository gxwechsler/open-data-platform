"""Time Series - Multi-indicator comparison."""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ingestion.unified_data import UnifiedData

st.set_page_config(page_title="Time Series | Open Data Platform", page_icon="📈", layout="wide")
st.title("📈 Time Series Analysis")

data = UnifiedData()

# Session state
for key, val in [('ts_sources', None), ('ts_categories', None), ('ts_indicators', None),
                 ('ts_countries', None), ('ts_yr_s', 1990), ('ts_yr_e', 2024),
                 ('ts_normalize', False), ('ts_markers', True), ('ts_dual', False)]:
    if key not in st.session_state:
        st.session_state[key] = val

st.sidebar.header("Configuration")

# Sources
sources = data.get_sources()
if st.session_state.ts_sources is None:
    st.session_state.ts_sources = sources
valid = [s for s in st.session_state.ts_sources if s in sources] or sources
sel_sources = st.sidebar.multiselect("Sources", sources, default=valid, key="ts_src")
st.session_state.ts_sources = sel_sources
if not sel_sources:
    st.warning("Select at least one source.")
    st.stop()

# Categories
all_cats = sorted(set(c for s in sel_sources for c in data.get_categories(source=s)))
if st.session_state.ts_categories is None:
    st.session_state.ts_categories = all_cats[:3]
valid = [c for c in st.session_state.ts_categories if c in all_cats] or all_cats[:3]
sel_cats = st.sidebar.multiselect("Categories", all_cats, default=valid, key="ts_cat")
st.session_state.ts_categories = sel_cats
if not sel_cats:
    st.warning("Select at least one category.")
    st.stop()

# Indicators
all_inds = []
for s in sel_sources:
    for c in sel_cats:
        all_inds.extend(data.get_indicators(source=s, category=c))
seen = set()
unique = [i for i in all_inds if i['indicator_code'] not in seen and not seen.add(i['indicator_code'])]
if not unique:
    st.warning("No indicators found.")
    st.stop()

ind_opts = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in unique}
ind_units = {i['indicator_code']: i.get('units', '') for i in unique}

if st.session_state.ts_indicators is None:
    st.session_state.ts_indicators = [list(ind_opts.keys())[0]]
valid = [i for i in st.session_state.ts_indicators if i in ind_opts] or [list(ind_opts.keys())[0]]

sel_inds = st.sidebar.multiselect("Indicators (max 4)", list(ind_opts.keys()), default=valid,
    format_func=lambda x: ind_opts.get(x, x), max_selections=4, key="ts_ind")
st.session_state.ts_indicators = sel_inds
if not sel_inds:
    st.info("Select at least one indicator.")
    st.stop()

st.sidebar.markdown("**Selected:**")
for i in sel_inds:
    st.sidebar.caption(f"• {ind_opts.get(i, i)[:50]}")

# Countries
countries = data.get_countries()
co_opts = {c['country_iso3']: c['country_name'] for c in countries}
if st.session_state.ts_countries is None:
    st.session_state.ts_countries = [c for c in ["USA", "CHN", "DEU", "JPN", "BRA"] if c in co_opts][:5]
valid = [c for c in st.session_state.ts_countries if c in co_opts]
if not valid:
    valid = [c for c in ["USA", "CHN", "DEU", "JPN", "BRA"] if c in co_opts][:5]

sel_cos = st.sidebar.multiselect("Countries", list(co_opts.keys()), default=valid,
    format_func=lambda x: co_opts.get(x, x), key="ts_co")
st.session_state.ts_countries = sel_cos
if not sel_cos:
    st.warning("Select at least one country.")
    st.stop()

# Year range
mn, mx = data.get_year_range()
yr = st.sidebar.slider("Years", mn, mx, (max(mn, st.session_state.ts_yr_s), min(mx, st.session_state.ts_yr_e)), key="ts_yr")
st.session_state.ts_yr_s, st.session_state.ts_yr_e = yr

# Options
st.sidebar.markdown("---")
norm = st.sidebar.checkbox("Normalize (Index=100)", st.session_state.ts_normalize, key="ts_norm")
st.session_state.ts_normalize = norm
markers = st.sidebar.checkbox("Show Markers", st.session_state.ts_markers, key="ts_mark")
st.session_state.ts_markers = markers
dual = st.sidebar.checkbox("Dual Y-Axis", st.session_state.ts_dual, key="ts_dual")
st.session_state.ts_dual = dual

# Fetch data
all_data = []
for ind in sel_inds:
    df = data.get_data(indicator_code=ind, countries=sel_cos, year_start=yr[0], year_end=yr[1])
    if not df.empty:
        all_data.append(df)
if not all_data:
    st.warning("No data found.")
    st.stop()

combined = pd.concat(all_data, ignore_index=True)
mode = 'lines+markers' if markers else 'lines'
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

def normalize_df(df):
    if norm:
        for c in df['country_iso3'].unique():
            m = df['country_iso3'] == c
            first = df.loc[m].sort_values('year')['value'].iloc[0]
            if first and first != 0:
                df.loc[m, 'value'] = df.loc[m, 'value'] / first * 100
    return df

if len(sel_inds) == 1:
    ind = sel_inds[0]
    st.markdown(f"### {ind_opts.get(ind, ind)}")
    df = normalize_df(all_data[0].copy())
    fig = go.Figure()
    for c in df['country_name'].unique():
        cdf = df[df['country_name'] == c].sort_values('year')
        fig.add_trace(go.Scatter(x=cdf['year'], y=cdf['value'], mode=mode, name=c))
    fig.update_layout(xaxis_title="Year", yaxis_title="Index" if norm else ind_units.get(ind, 'Value'),
                     hovermode="x unified", height=500)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.markdown("### Multi-Indicator Comparison")
    if len(sel_cos) == 1:
        st.markdown(f"**Country:** {co_opts.get(sel_cos[0])}")
        if dual and len(sel_inds) >= 2:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            half = len(sel_inds) // 2 + len(sel_inds) % 2
            for i, ind in enumerate(sel_inds):
                df = [d for d in all_data if d['indicator_code'].iloc[0] == ind]
                if df:
                    df = normalize_df(df[0].copy()).sort_values('year')
                    sec = i >= half
                    fig.add_trace(go.Scatter(x=df['year'], y=df['value'], mode=mode,
                        name=ind_opts.get(ind, ind).split(' (')[0][:30], line=dict(color=colors[i%4])), secondary_y=sec)
            fig.update_layout(xaxis_title="Year", hovermode="x unified", height=500)
            fig.update_yaxes(title_text="Left Axis", secondary_y=False)
            fig.update_yaxes(title_text="Right Axis", secondary_y=True)
        else:
            fig = go.Figure()
            for i, ind in enumerate(sel_inds):
                df = [d for d in all_data if d['indicator_code'].iloc[0] == ind]
                if df:
                    df = normalize_df(df[0].copy()).sort_values('year')
                    fig.add_trace(go.Scatter(x=df['year'], y=df['value'], mode=mode,
                        name=ind_opts.get(ind, ind).split(' (')[0][:30], line=dict(color=colors[i%4])))
            fig.update_layout(xaxis_title="Year", yaxis_title="Index" if norm else "Value",
                             hovermode="x unified", height=500)
        st.plotly_chart(fig, use_container_width=True)
    else:
        ind = sel_inds[0]
        st.markdown(f"**Indicator:** {ind_opts.get(ind, ind)}")
        df = normalize_df(all_data[0].copy())
        fig = go.Figure()
        for c in df['country_name'].unique():
            cdf = df[df['country_name'] == c].sort_values('year')
            fig.add_trace(go.Scatter(x=cdf['year'], y=cdf['value'], mode=mode, name=c))
        fig.update_layout(xaxis_title="Year", yaxis_title="Index" if norm else ind_units.get(ind, 'Value'),
                         hovermode="x unified", height=500)
        st.plotly_chart(fig, use_container_width=True)
        if len(sel_inds) > 1:
            st.info("💡 Select one country to compare multiple indicators.")

st.markdown("---")
disp = combined[['year', 'country_name', 'indicator_name', 'value', 'source']].sort_values(['indicator_name', 'country_name', 'year'])
st.dataframe(disp, use_container_width=True, hide_index=True)
st.download_button("📥 CSV", disp.to_csv(index=False), "timeseries.csv", "text/csv")
