"""Crisis Analysis - Double and Triple Crisis Detection."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Crisis Analysis | Open Data Platform", page_icon="⚠️", layout="wide")
st.title("⚠️ Crisis Analysis")
st.markdown("Analyze concurrent crises: Banking, Currency, and Sovereign Debt")

db = get_db_manager()

# --- Initialize logical state ---
if 'saved_ca_cos' not in st.session_state:
    st.session_state.saved_ca_cos = None
if 'saved_ca_yr' not in st.session_state:
    st.session_state.saved_ca_yr = None

@st.cache_data(ttl=300)
def get_crisis_indicators():
    """Get crisis type indicators (banking, currency, sovereign)"""
    r = db.execute_query("""
        SELECT DISTINCT indicator_code, indicator_name, source
        FROM time_series_unified_data 
        WHERE source IN ('LV', 'RR')
        AND (
            LOWER(indicator_name) LIKE '%banking%' OR
            LOWER(indicator_name) LIKE '%currency%' OR
            LOWER(indicator_name) LIKE '%sovereign%' OR
            LOWER(indicator_name) LIKE '%debt%' OR
            LOWER(indicator_code) LIKE '%bank%' OR
            LOWER(indicator_code) LIKE '%currency%' OR
            LOWER(indicator_code) LIKE '%sovereign%'
        )
        ORDER BY indicator_name
    """)
    return r if r else []

@st.cache_data(ttl=300)
def get_all_crisis_data():
    """Get all binary crisis indicators"""
    r = db.execute_query("""
        SELECT country_iso3, country_name, year, indicator_code, indicator_name, value, source
        FROM time_series_unified_data 
        WHERE source IN ('LV', 'RR')
        ORDER BY country_name, year
    """)
    return pd.DataFrame(r) if r else pd.DataFrame()

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

# --- Sidebar ---
st.sidebar.header("Filters")

countries = get_countries()
co_opts = {c['country_iso3']: c['country_name'] for c in countries}
co_codes = list(co_opts.keys())

if st.session_state.saved_ca_cos is None:
    st.session_state.saved_ca_cos = co_codes  # All countries by default
default_cos = [c for c in st.session_state.saved_ca_cos if c in co_codes] or co_codes

sel_cos = st.sidebar.multiselect("Countries", co_codes, default=default_cos,
    format_func=lambda x: co_opts.get(x, x), key="widget_ca_co")
st.session_state.saved_ca_cos = sel_cos

mn, mx = get_years()
if st.session_state.saved_ca_yr is None:
    st.session_state.saved_ca_yr = (1970, mx)
default_yr = (max(mn, st.session_state.saved_ca_yr[0]), min(mx, st.session_state.saved_ca_yr[1]))

yr = st.sidebar.slider("Years", mn, mx, default_yr, key="widget_ca_yr")
st.session_state.saved_ca_yr = yr

if not sel_cos:
    st.warning("Select at least one country.")
    st.stop()

# --- Load and process data ---
df = get_all_crisis_data()

if df.empty:
    st.error("No crisis data found in database.")
    st.stop()

# Filter by selections
df = df[df['country_iso3'].isin(sel_cos)]
df = df[(df['year'] >= yr[0]) & (df['year'] <= yr[1])]

# Classify crisis types
def classify_crisis(indicator_name, indicator_code):
    name_lower = (indicator_name or '').lower()
    code_lower = (indicator_code or '').lower()
    
    if 'banking' in name_lower or 'bank' in code_lower:
        return 'Banking'
    elif 'currency' in name_lower or 'currency' in code_lower:
        return 'Currency'
    elif 'sovereign' in name_lower or 'debt' in name_lower or 'sovereign' in code_lower:
        return 'Sovereign'
    else:
        return 'Other'

df['crisis_type'] = df.apply(lambda r: classify_crisis(r['indicator_name'], r['indicator_code']), axis=1)

# Filter to binary crisis indicators (value = 1 means crisis)
crisis_events = df[(df['value'] == 1) & (df['crisis_type'] != 'Other')].copy()

if crisis_events.empty:
    st.info("No crisis events found for selected filters.")
    st.stop()

# --- Analyze concurrent crises ---
st.markdown("### 📊 Crisis Overview")

col1, col2, col3, col4 = st.columns(4)
banking = crisis_events[crisis_events['crisis_type'] == 'Banking']
currency = crisis_events[crisis_events['crisis_type'] == 'Currency']
sovereign = crisis_events[crisis_events['crisis_type'] == 'Sovereign']

col1.metric("Banking Crises", len(banking))
col2.metric("Currency Crises", len(currency))
col3.metric("Sovereign Crises", len(sovereign))
col4.metric("Total Events", len(crisis_events))

# --- Find concurrent crises ---
st.markdown("---")
st.markdown("### 🔗 Concurrent Crises Analysis")
st.markdown("Identifying years when multiple crisis types occurred simultaneously in the same country.")

# Pivot: country-year with crisis types
pivot = crisis_events.pivot_table(
    index=['country_iso3', 'country_name', 'year'],
    columns='crisis_type',
    values='value',
    aggfunc='max'
).reset_index()

# Fill NaN with 0
for col in ['Banking', 'Currency', 'Sovereign']:
    if col not in pivot.columns:
        pivot[col] = 0
    pivot[col] = pivot[col].fillna(0).astype(int)

# Count concurrent crises
pivot['crisis_count'] = pivot['Banking'] + pivot['Currency'] + pivot['Sovereign']
pivot['crisis_label'] = pivot['crisis_count'].map({
    1: 'Single Crisis',
    2: 'Double Crisis',
    3: 'Triple Crisis'
})

# Filter to multi-crisis events
double_crises = pivot[pivot['crisis_count'] == 2]
triple_crises = pivot[pivot['crisis_count'] == 3]

tab1, tab2, tab3, tab4 = st.tabs(["📈 Timeline", "🔴 Double Crises", "⚫ Triple Crises", "📋 Data"])

with tab1:
    # Stacked timeline of crisis types
    timeline_df = crisis_events.groupby(['year', 'crisis_type']).size().reset_index(name='count')
    
    fig = px.bar(timeline_df, x='year', y='count', color='crisis_type',
        title="Crisis Events Over Time by Type",
        labels={'count': 'Number of Countries', 'year': 'Year', 'crisis_type': 'Crisis Type'},
        color_discrete_map={'Banking': '#e74c3c', 'Currency': '#f39c12', 'Sovereign': '#9b59b6'})
    fig.update_layout(hovermode="x unified", height=400, barmode='stack')
    st.plotly_chart(fig, use_container_width=True)
    
    # Country distribution
    country_counts = crisis_events.groupby('country_name')['crisis_type'].count().sort_values(ascending=True).tail(20)
    fig2 = px.bar(x=country_counts.values, y=country_counts.index, orientation='h',
        title="Top 20 Countries by Crisis Frequency",
        labels={'x': 'Number of Crisis Events', 'y': 'Country'})
    fig2.update_layout(height=500)
    st.plotly_chart(fig2, use_container_width=True)

with tab2:
    st.markdown("#### Double Crises (2 concurrent types)")
    if len(double_crises) > 0:
        # Identify which pairs
        double_crises['pair'] = double_crises.apply(
            lambda r: '+'.join(sorted([t for t in ['Banking', 'Currency', 'Sovereign'] if r[t] == 1])), axis=1)
        
        col1, col2 = st.columns([1, 2])
        with col1:
            pair_counts = double_crises['pair'].value_counts()
            st.markdown("**Crisis Pairs:**")
            for pair, count in pair_counts.items():
                st.write(f"• {pair}: **{count}** events")
        
        with col2:
            fig = px.scatter(double_crises, x='year', y='country_name', color='pair',
                title="Double Crisis Events",
                hover_data=['Banking', 'Currency', 'Sovereign'])
            fig.update_traces(marker=dict(size=12))
            fig.update_layout(height=max(300, len(double_crises['country_name'].unique()) * 20))
            st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(double_crises[['year', 'country_name', 'Banking', 'Currency', 'Sovereign', 'pair']].sort_values(['year', 'country_name']),
            use_container_width=True, hide_index=True)
    else:
        st.info("No double crisis events found in selected range.")

with tab3:
    st.markdown("#### Triple Crises (Banking + Currency + Sovereign)")
    if len(triple_crises) > 0:
        st.error(f"⚠️ Found **{len(triple_crises)}** triple crisis events!")
        
        fig = px.scatter(triple_crises, x='year', y='country_name',
            title="Triple Crisis Events (Most Severe)",
            hover_data=['Banking', 'Currency', 'Sovereign'])
        fig.update_traces(marker=dict(size=15, color='black', symbol='x'))
        fig.update_layout(height=max(300, len(triple_crises['country_name'].unique()) * 25))
        st.plotly_chart(fig, use_container_width=True)
        
        st.dataframe(triple_crises[['year', 'country_name', 'Banking', 'Currency', 'Sovereign']].sort_values(['year', 'country_name']),
            use_container_width=True, hide_index=True)
    else:
        st.success("✅ No triple crisis events found in selected range.")

with tab4:
    st.markdown("#### All Crisis Data")
    disp = pivot[['year', 'country_name', 'Banking', 'Currency', 'Sovereign', 'crisis_count', 'crisis_label']]
    disp = disp.sort_values(['year', 'country_name'])
    st.dataframe(disp, use_container_width=True, hide_index=True)
    st.download_button("📥 CSV", disp.to_csv(index=False), "crisis_analysis.csv", "text/csv")

# --- Summary stats ---
st.markdown("---")
st.markdown("### 📈 Summary Statistics")

col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Single Crises", len(pivot[pivot['crisis_count'] == 1]))
with col2:
    st.metric("Double Crises", len(double_crises))
with col3:
    st.metric("Triple Crises", len(triple_crises))

st.caption("**Methodology:** A concurrent crisis is when multiple crisis types (Banking, Currency, Sovereign) occur in the same country-year.")
st.caption("**Sources:** Laeven-Valencia (IMF), Reinhart-Rogoff historical database")
