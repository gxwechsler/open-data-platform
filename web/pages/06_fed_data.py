"""Federal Reserve Data - FRED economic indicators."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Fed Data | Open Data Platform", page_icon="🏛️", layout="wide")
st.title("🏛️ Federal Reserve Economic Data")

db = get_db_manager()

# Session state
for key, val in [('fed_mode', 'Dashboard'), ('fed_yr_s', 1990), ('fed_yr_e', 2024),
                 ('fed_ind', None), ('fed_ind1', None), ('fed_ind2', None), ('fed_norm', False)]:
    if key not in st.session_state:
        st.session_state[key] = val

@st.cache_data(ttl=300)
def get_indicators():
    r = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name, units, category
        FROM time_series_unified_data WHERE source = 'FRED' ORDER BY indicator_name
    """)
    return r if r else []

@st.cache_data(ttl=300)
def get_years():
    r = db.execute_query("SELECT MIN(year) as mn, MAX(year) as mx FROM time_series_unified_data WHERE source = 'FRED'")
    return (int(r[0]['mn']), int(r[0]['mx'])) if r and r[0]['mn'] else (1950, 2025)

def get_data(indicator=None, yr_s=None, yr_e=None):
    q = "SELECT * FROM time_series_unified_data WHERE source = 'FRED'"
    p = {}
    if indicator:
        q += " AND indicator_code = :ind"
        p['ind'] = indicator
    if yr_s:
        q += " AND year >= :ys"
        p['ys'] = yr_s
    if yr_e:
        q += " AND year <= :ye"
        p['ye'] = yr_e
    q += " ORDER BY year"
    return pd.DataFrame(db.execute_query(q, p or None) or [])

indicators = get_indicators()
if not indicators:
    st.warning("No FRED data found.")
    st.stop()

ind_opts = {i['indicator_code']: i['indicator_name'] for i in indicators}
ind_units = {i['indicator_code']: i.get('units', '') for i in indicators}
mn_yr, mx_yr = get_years()

st.sidebar.header("Configuration")

modes = ['Dashboard', 'Single Indicator', 'Compare Two']
idx = modes.index(st.session_state.fed_mode) if st.session_state.fed_mode in modes else 0
mode = st.sidebar.radio("Mode", modes, index=idx, key="fed_mode_sel")
st.session_state.fed_mode = mode

yr = st.sidebar.slider("Years", mn_yr, mx_yr,
    (max(mn_yr, st.session_state.fed_yr_s), min(mx_yr, st.session_state.fed_yr_e)), key="fed_yr")
st.session_state.fed_yr_s, st.session_state.fed_yr_e = yr

if mode == 'Dashboard':
    st.markdown("### Key Indicators")
    key_inds = ['GDPC1', 'UNRATE', 'CPIAUCSL', 'FEDFUNDS', 'DGS10', 'M2SL']
    available = [k for k in key_inds if k in ind_opts]
    
    if available:
        cols = st.columns(min(3, len(available)))
        for i, ind in enumerate(available[:6]):
            df = get_data(ind, yr[0], yr[1])
            if not df.empty:
                with cols[i % 3]:
                    st.markdown(f"**{ind_opts.get(ind, ind)[:25]}**")
                    latest = df.iloc[-1]['value']
                    prev = df.iloc[-2]['value'] if len(df) > 1 else latest
                    delta = ((latest - prev) / prev * 100) if prev and prev != 0 else 0
                    st.metric("Latest", f"{latest:,.2f}", f"{delta:+.1f}%")
                    fig = px.line(df, x='year', y='value', height=120)
                    fig.update_layout(margin=dict(l=0, r=0, t=0, b=0), showlegend=False, xaxis_title="", yaxis_title="")
                    st.plotly_chart(fig, use_container_width=True)
    
    st.markdown("---")
    st.markdown("### All FRED Indicators")
    st.dataframe(pd.DataFrame(indicators)[['indicator_code', 'indicator_name', 'units', 'category']],
                use_container_width=True, hide_index=True)

elif mode == 'Single Indicator':
    codes = list(ind_opts.keys())
    if st.session_state.fed_ind not in codes:
        st.session_state.fed_ind = codes[0]
    idx = codes.index(st.session_state.fed_ind)
    ind = st.sidebar.selectbox("Indicator", codes, index=idx, format_func=lambda x: ind_opts.get(x, x), key="fed_ind")
    st.session_state.fed_ind = ind
    
    df = get_data(ind, yr[0], yr[1])
    if not df.empty:
        st.markdown(f"### {ind_opts.get(ind, ind)}")
        
        col1, col2, col3 = st.columns(3)
        col1.metric("Latest", f"{df.iloc[-1]['value']:,.2f}")
        col2.metric("Min", f"{df['value'].min():,.2f}")
        col3.metric("Max", f"{df['value'].max():,.2f}")
        
        fig = px.line(df, x='year', y='value', markers=True)
        fig.update_layout(xaxis_title="Year", yaxis_title=ind_units.get(ind, 'Value'), hovermode="x unified", height=450)
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(df[['year', 'value']].sort_values('year', ascending=False), use_container_width=True, hide_index=True)
        st.download_button("📥 CSV", df.to_csv(index=False), f"{ind}.csv", "text/csv")

else:  # Compare Two
    codes = list(ind_opts.keys())
    if len(codes) < 2:
        st.warning("Need at least 2 indicators.")
        st.stop()
    
    col1, col2 = st.sidebar.columns(2)
    with col1:
        if st.session_state.fed_ind1 not in codes:
            st.session_state.fed_ind1 = codes[0]
        idx1 = codes.index(st.session_state.fed_ind1)
        ind1 = st.selectbox("Indicator 1", codes, index=idx1, format_func=lambda x: ind_opts.get(x, x)[:20], key="fed_i1")
        st.session_state.fed_ind1 = ind1
    with col2:
        if st.session_state.fed_ind2 not in codes:
            st.session_state.fed_ind2 = codes[1]
        idx2 = codes.index(st.session_state.fed_ind2)
        ind2 = st.selectbox("Indicator 2", codes, index=idx2, format_func=lambda x: ind_opts.get(x, x)[:20], key="fed_i2")
        st.session_state.fed_ind2 = ind2
    
    norm = st.sidebar.checkbox("Normalize", st.session_state.fed_norm, key="fed_norm_cb")
    st.session_state.fed_norm = norm
    
    df1 = get_data(ind1, yr[0], yr[1])
    df2 = get_data(ind2, yr[0], yr[1])
    
    if not df1.empty and not df2.empty:
        st.markdown(f"### {ind_opts.get(ind1, ind1)[:25]} vs {ind_opts.get(ind2, ind2)[:25]}")
        
        if norm:
            b1, b2 = df1.iloc[0]['value'], df2.iloc[0]['value']
            if b1 and b1 != 0:
                df1['value'] = df1['value'] / b1 * 100
            if b2 and b2 != 0:
                df2['value'] = df2['value'] / b2 * 100
        
        fig = go.Figure()
        fig.add_trace(go.Scatter(x=df1['year'], y=df1['value'], mode='lines+markers',
            name=ind_opts.get(ind1, ind1)[:20], yaxis='y1'))
        fig.add_trace(go.Scatter(x=df2['year'], y=df2['value'], mode='lines+markers',
            name=ind_opts.get(ind2, ind2)[:20], yaxis='y2'))
        
        fig.update_layout(
            xaxis_title="Year",
            yaxis=dict(title=ind_units.get(ind1, '') if not norm else "Index", side='left'),
            yaxis2=dict(title=ind_units.get(ind2, '') if not norm else "Index", side='right', overlaying='y'),
            hovermode="x unified", height=450, legend=dict(x=0, y=1.1, orientation='h')
        )
        st.plotly_chart(fig, use_container_width=True)

st.markdown("---")
st.caption("Source: Federal Reserve Economic Data (FRED)")
