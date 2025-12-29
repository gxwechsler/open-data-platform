"""Time Series - Multi-indicator comparison."""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ingestion.unified_data import UnifiedData

st.set_page_config(page_title="Time Series | Open Data Platform", page_icon="📈", layout="wide")
st.title("📈 Time Series Analysis")

data = UnifiedData()

# --- Initialize logical state ---
if 'initialized_ts' not in st.session_state:
    st.session_state.initialized_ts = False
    st.session_state.saved_ts_sources = []
    st.session_state.saved_ts_cats = []
    st.session_state.saved_ts_inds = []
    st.session_state.saved_ts_cos = []
    st.session_state.saved_ts_yr = None
    st.session_state.saved_ts_norm = False
    st.session_state.saved_ts_markers = True
    st.session_state.saved_ts_dual = False

st.sidebar.header("Configuration")

# --- Sources ---
sources = data.get_sources()
default_src = [s for s in st.session_state.saved_ts_sources if s in sources]

sel_sources = st.sidebar.multiselect("Sources", sources, default=default_src, key="widget_ts_src")
st.session_state.saved_ts_sources = sel_sources

if not sel_sources:
    st.info("👈 Select at least one **Source** to begin.")
    st.stop()

# --- Categories ---
all_cats = sorted(set(c for s in sel_sources for c in data.get_categories(source=s)))
default_cats = [c for c in st.session_state.saved_ts_cats if c in all_cats]

sel_cats = st.sidebar.multiselect("Categories", all_cats, default=default_cats, key="widget_ts_cat")
st.session_state.saved_ts_cats = sel_cats

if not sel_cats:
    st.info("👈 Select at least one **Category**.")
    st.stop()

# --- Indicators ---
all_inds = []
for s in sel_sources:
    for c in sel_cats:
        all_inds.extend(data.get_indicators(source=s, category=c))
seen = set()
unique = [i for i in all_inds if i['indicator_code'] not in seen and not seen.add(i['indicator_code'])]

if not unique:
    st.warning("No indicators found for selected sources/categories.")
    st.stop()

# Build indicator metadata dictionaries
ind_opts = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in unique}
ind_units = {}
for i in unique:
    unit = i.get('units') or ''
    if not unit or unit.strip() == '':
        # Infer from indicator name if possible
        name = i.get('indicator_name', '').lower()
        if 'gdp' in name and 'capita' in name:
            unit = 'USD per capita'
        elif 'gdp' in name:
            unit = 'USD'
        elif 'percent' in name or 'rate' in name or '%' in name:
            unit = '%'
        elif 'population' in name:
            unit = 'Persons'
        else:
            unit = 'Value'
    ind_units[i['indicator_code']] = unit

ind_codes = list(ind_opts.keys())
default_inds = [i for i in st.session_state.saved_ts_inds if i in ind_codes]

sel_inds = st.sidebar.multiselect("Indicators (max 4)", ind_codes, default=default_inds,
    format_func=lambda x: ind_opts.get(x, x), max_selections=4, key="widget_ts_ind")
st.session_state.saved_ts_inds = sel_inds

if not sel_inds:
    st.info("👈 Select at least one **Indicator**.")
    st.stop()

st.sidebar.markdown("**Selected:**")
for i in sel_inds:
    unit = ind_units.get(i, 'Value')
    st.sidebar.caption(f"• {ind_opts.get(i, i)[:40]}... [{unit}]")

# --- Countries ---
countries = data.get_countries()
co_opts = {c['country_iso3']: c['country_name'] for c in countries}
co_codes = list(co_opts.keys())
default_cos = [c for c in st.session_state.saved_ts_cos if c in co_codes]

sel_cos = st.sidebar.multiselect("Countries", co_codes, default=default_cos,
    format_func=lambda x: co_opts.get(x, x), key="widget_ts_co")
st.session_state.saved_ts_cos = sel_cos

if not sel_cos:
    st.info("👈 Select at least one **Country**.")
    st.stop()

st.session_state.initialized_ts = True

# --- Year range ---
mn, mx = data.get_year_range()
if st.session_state.saved_ts_yr is None:
    st.session_state.saved_ts_yr = (mn, mx)
default_yr = (max(mn, st.session_state.saved_ts_yr[0]), min(mx, st.session_state.saved_ts_yr[1]))

yr = st.sidebar.slider("Years", mn, mx, default_yr, key="widget_ts_yr")
st.session_state.saved_ts_yr = yr

# --- Options ---
st.sidebar.markdown("---")
norm = st.sidebar.checkbox("Normalize (Index=100)", value=st.session_state.saved_ts_norm, key="widget_ts_norm")
st.session_state.saved_ts_norm = norm

markers = st.sidebar.checkbox("Show Markers", value=st.session_state.saved_ts_markers, key="widget_ts_mark")
st.session_state.saved_ts_markers = markers

dual = st.sidebar.checkbox("Dual Y-Axis", value=st.session_state.saved_ts_dual, key="widget_ts_dual")
st.session_state.saved_ts_dual = dual

# --- Fetch data ---
all_data = []
for ind in sel_inds:
    df = data.get_data(indicator_code=ind, countries=sel_cos, year_start=yr[0], year_end=yr[1])
    if not df.empty:
        all_data.append(df)

if not all_data:
    st.warning("No data found for selected filters.")
    st.stop()

combined = pd.concat(all_data, ignore_index=True)
colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']

def normalize_df(df):
    if norm:
        result = df.copy()
        for c in result['country_iso3'].unique():
            m = result['country_iso3'] == c
            sorted_df = result.loc[m].sort_values('year')
            if len(sorted_df) > 0:
                first = sorted_df['value'].iloc[0]
                if first and first != 0:
                    result.loc[m, 'value'] = result.loc[m, 'value'] / first * 100
        return result
    return df

def is_sparse_data(df):
    """Detect if data is sparse (non-continuous years)"""
    if df.empty:
        return False
    years = sorted(df['year'].unique())
    if len(years) < 2:
        return True
    expected_years = set(range(min(years), max(years) + 1))
    actual_years = set(years)
    coverage = len(actual_years) / len(expected_years) if expected_years else 1
    return coverage < 0.5

def get_unit_label(indicator_code, normalized=False):
    """Get formatted unit label for axis"""
    if normalized:
        return "Index (base year = 100)"
    return ind_units.get(indicator_code, 'Value')

# --- Chart ---
if len(sel_inds) == 1:
    ind = sel_inds[0]
    unit_label = get_unit_label(ind, norm)
    
    st.markdown(f"### {ind_opts.get(ind, ind)}")
    st.caption(f"**Units:** {unit_label}")
    
    df = normalize_df(all_data[0].copy())
    chart_type = 'bar' if is_sparse_data(df) else 'line'
    
    if chart_type == 'bar':
        fig = px.bar(df, x='year', y='value', color='country_name', barmode='group',
            labels={'value': unit_label, 'year': 'Year', 'country_name': 'Country'})
    else:
        mode = 'lines+markers' if markers else 'lines'
        fig = go.Figure()
        for c in df['country_name'].unique():
            cdf = df[df['country_name'] == c].sort_values('year')
            fig.add_trace(go.Scatter(x=cdf['year'], y=cdf['value'], mode=mode, name=c))
        fig.update_layout(xaxis_title="Year", yaxis_title=unit_label)
    
    fig.update_layout(hovermode="x unified", height=500)
    st.plotly_chart(fig, use_container_width=True)

else:
    st.markdown("### Multi-Indicator Comparison")
    mode = 'lines+markers' if markers else 'lines'
    
    if len(sel_cos) == 1:
        st.markdown(f"**Country:** {co_opts.get(sel_cos[0])}")
        
        # Display units for all selected indicators
        st.caption("**Units:** " + " | ".join([f"{ind_opts.get(i, i).split(' (')[0][:20]}: {get_unit_label(i, norm)}" for i in sel_inds]))
        
        any_sparse = any(is_sparse_data(d) for d in all_data)
        
        if any_sparse:
            combined_df = pd.concat(all_data, ignore_index=True)
            combined_df = normalize_df(combined_df)
            combined_df['indicator_short'] = combined_df['indicator_code'].map(
                lambda x: f"{ind_opts.get(x, x).split(' (')[0][:20]} [{get_unit_label(x, norm)}]")
            fig = px.bar(combined_df, x='year', y='value', color='indicator_short', barmode='group',
                labels={'value': 'Value', 'year': 'Year'})
        elif dual and len(sel_inds) >= 2:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            half = len(sel_inds) // 2 + len(sel_inds) % 2
            for i, ind in enumerate(sel_inds):
                df = [d for d in all_data if d['indicator_code'].iloc[0] == ind]
                if df:
                    df = normalize_df(df[0].copy()).sort_values('year')
                    sec = i >= half
                    unit_label = get_unit_label(ind, norm)
                    fig.add_trace(go.Scatter(x=df['year'], y=df['value'], mode=mode,
                        name=f"{ind_opts.get(ind, ind).split(' (')[0][:25]}",
                        line=dict(color=colors[i%4])), secondary_y=sec)
            fig.update_layout(xaxis_title="Year", hovermode="x unified", height=500)
            fig.update_yaxes(title_text=get_unit_label(sel_inds[0], norm), secondary_y=False)
            fig.update_yaxes(title_text=get_unit_label(sel_inds[half], norm), secondary_y=True)
        else:
            fig = go.Figure()
            for i, ind in enumerate(sel_inds):
                df = [d for d in all_data if d['indicator_code'].iloc[0] == ind]
                if df:
                    df = normalize_df(df[0].copy()).sort_values('year')
                    unit_label = get_unit_label(ind, norm)
                    fig.add_trace(go.Scatter(x=df['year'], y=df['value'], mode=mode,
                        name=f"{ind_opts.get(ind, ind).split(' (')[0][:25]} [{unit_label}]",
                        line=dict(color=colors[i%4])))
            fig.update_layout(xaxis_title="Year", yaxis_title="Value (see legend for units)",
                             hovermode="x unified", height=500)
        
        st.plotly_chart(fig, use_container_width=True)
    else:
        ind = sel_inds[0]
        unit_label = get_unit_label(ind, norm)
        
        st.markdown(f"**Indicator:** {ind_opts.get(ind, ind)}")
        st.caption(f"**Units:** {unit_label}")
        
        df = normalize_df(all_data[0].copy())
        chart_type = 'bar' if is_sparse_data(df) else 'line'
        
        if chart_type == 'bar':
            fig = px.bar(df, x='year', y='value', color='country_name', barmode='group',
                labels={'value': unit_label, 'year': 'Year', 'country_name': 'Country'})
        else:
            fig = go.Figure()
            for c in df['country_name'].unique():
                cdf = df[df['country_name'] == c].sort_values('year')
                fig.add_trace(go.Scatter(x=cdf['year'], y=cdf['value'], mode=mode, name=c))
            fig.update_layout(xaxis_title="Year", yaxis_title=unit_label)
        
        fig.update_layout(hovermode="x unified", height=500)
        st.plotly_chart(fig, use_container_width=True)
        
        if len(sel_inds) > 1:
            st.info("💡 Select one country to compare multiple indicators.")

st.markdown("---")

# Show units in data table
disp = combined[['year', 'country_name', 'indicator_name', 'value', 'source']].copy()
disp['units'] = combined['indicator_code'].map(ind_units)
disp = disp.sort_values(['indicator_name', 'country_name', 'year'])
st.dataframe(disp, use_container_width=True, hide_index=True)
st.download_button("📥 CSV", disp.to_csv(index=False), "timeseries.csv", "text/csv")
