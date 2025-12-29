"""Economic Crisis page - Financial crises database."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Economic Crisis | Open Data Platform", page_icon="🏦", layout="wide")
st.title("🏦 Economic Crisis Database")

db = get_db_manager()

COUNTRY_NAMES = {
    "ARG": "Argentina", "AUS": "Australia", "AUT": "Austria", "BEL": "Belgium",
    "BRA": "Brazil", "CAN": "Canada", "CHL": "Chile", "CHN": "China",
    "COL": "Colombia", "CZE": "Czech Republic", "DEU": "Germany", "DNK": "Denmark",
    "ESP": "Spain", "FIN": "Finland", "FRA": "France", "GBR": "United Kingdom",
    "GRC": "Greece", "HUN": "Hungary", "IDN": "Indonesia", "IND": "India",
    "IRL": "Ireland", "ISL": "Iceland", "ITA": "Italy", "JPN": "Japan",
    "KOR": "South Korea", "MEX": "Mexico", "MYS": "Malaysia", "NLD": "Netherlands",
    "NOR": "Norway", "NZL": "New Zealand", "PHL": "Philippines", "POL": "Poland",
    "PRT": "Portugal", "RUS": "Russia", "SWE": "Sweden", "THA": "Thailand",
    "TUR": "Turkey", "TWN": "Taiwan", "USA": "United States", "VEN": "Venezuela",
    "ZAF": "South Africa", "ZWE": "Zimbabwe"
}

if 'ec_crisis_type' not in st.session_state:
    st.session_state.ec_crisis_type = "All"
if 'ec_source' not in st.session_state:
    st.session_state.ec_source = "All"
if 'ec_year_start' not in st.session_state:
    st.session_state.ec_year_start = 1980
if 'ec_year_end' not in st.session_state:
    st.session_state.ec_year_end = 2024

@st.cache_data(ttl=300)
def get_crisis_types():
    result = db.execute_query("SELECT DISTINCT crisis_type FROM financial_crises WHERE crisis_type IS NOT NULL ORDER BY crisis_type")
    return [r['crisis_type'] for r in result] if result else []

@st.cache_data(ttl=300)
def get_sources():
    result = db.execute_query("SELECT DISTINCT source FROM financial_crises WHERE source IS NOT NULL ORDER BY source")
    return [r['source'] for r in result] if result else []

@st.cache_data(ttl=300)
def get_year_range():
    result = db.execute_query("SELECT MIN(start_year) as min_year, MAX(start_year) as max_year FROM financial_crises")
    if result and result[0]['min_year']:
        return int(result[0]['min_year']), int(result[0]['max_year'])
    return 1800, 2024

st.sidebar.header("Filters")

crisis_types = get_crisis_types()
if crisis_types:
    ctype_options = ["All"] + crisis_types
    crisis_type = st.sidebar.selectbox(
        "Crisis Type", 
        ctype_options,
        index=ctype_options.index(st.session_state.ec_crisis_type) if st.session_state.ec_crisis_type in ctype_options else 0,
        key="ec_crisis_type_select"
    )
    st.session_state.ec_crisis_type = crisis_type
else:
    crisis_type = "All"

sources = get_sources()
if sources:
    source_options = ["All"] + sources
    source = st.sidebar.selectbox(
        "Data Source", 
        source_options,
        index=source_options.index(st.session_state.ec_source) if st.session_state.ec_source in source_options else 0,
        key="ec_source_select"
    )
    st.session_state.ec_source = source
else:
    source = "All"

min_year, max_year = get_year_range()
year_range = st.sidebar.slider(
    "Year Range", 
    min_year, max_year, 
    (max(min_year, st.session_state.ec_year_start), min(max_year, st.session_state.ec_year_end)),
    key="ec_year_slider"
)
st.session_state.ec_year_start = year_range[0]
st.session_state.ec_year_end = year_range[1]

def get_crisis_data(crisis_type=None, source=None, year_start=None, year_end=None):
    query = "SELECT * FROM financial_crises WHERE 1=1"
    params = {}
    if crisis_type:
        query += " AND crisis_type = :ctype"
        params['ctype'] = crisis_type
    if source:
        query += " AND source = :source"
        params['source'] = source
    if year_start:
        query += " AND start_year >= :year_start"
        params['year_start'] = year_start
    if year_end:
        query += " AND start_year <= :year_end"
        params['year_end'] = year_end
    query += " ORDER BY start_year DESC"
    result = db.execute_query(query, params if params else None)
    if result:
        df = pd.DataFrame(result)
        df['country'] = df['country_iso3'].map(lambda x: COUNTRY_NAMES.get(x, x))
        return df
    return pd.DataFrame()

df = get_crisis_data(
    crisis_type=crisis_type if crisis_type != "All" else None,
    source=source if source != "All" else None,
    year_start=year_range[0],
    year_end=year_range[1]
)

if df.empty:
    st.warning("No crises found with the current filters.")
    st.info("💡 For more crisis data, check **Time Series** and filter by sources: LV (Laeven-Valencia) or RR (Reinhart-Rogoff)")
else:
    col1, col2, col3, col4 = st.columns(4)
    with col1:
        st.metric("Total Crises", len(df))
    with col2:
        st.metric("Countries Affected", df['country_iso3'].nunique())
    with col3:
        avg_loss = df['output_loss_pct'].mean() if 'output_loss_pct' in df.columns else None
        st.metric("Avg Output Loss", f"{avg_loss:.1f}%" if pd.notna(avg_loss) else "N/A")
    with col4:
        avg_cost = df['fiscal_cost_pct'].mean() if 'fiscal_cost_pct' in df.columns else None
        st.metric("Avg Fiscal Cost", f"{avg_cost:.1f}%" if pd.notna(avg_cost) else "N/A")
    
    tab1, tab2, tab3 = st.tabs(["📊 Timeline", "📈 Analysis", "📋 Data"])
    
    with tab1:
        fig = px.scatter(df, x='start_year', y='country', color='crisis_type', 
                        title="Financial Crises Timeline",
                        hover_data=['source', 'description'] if 'description' in df.columns else ['source'])
        fig.update_traces(marker=dict(size=12))
        fig.update_layout(yaxis_title="Country", xaxis_title="Year")
        st.plotly_chart(fig, use_container_width=True)
        
        if 'start_year' in df.columns and len(df) > 1:
            df['decade'] = (df['start_year'] // 10) * 10
            decade_counts = df.groupby('decade').size().reset_index(name='count')
            fig = px.bar(decade_counts, x='decade', y='count', title="Crises by Decade")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            type_counts = df['crisis_type'].value_counts().reset_index()
            type_counts.columns = ['Crisis Type', 'Count']
            fig = px.pie(type_counts, values='Count', names='Crisis Type', title="By Type")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            if 'source' in df.columns:
                source_counts = df['source'].value_counts().reset_index()
                source_counts.columns = ['Source', 'Count']
                fig = px.pie(source_counts, values='Count', names='Source', title="By Source")
                st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        display_cols = ['start_year', 'end_year', 'country', 'crisis_type', 'source', 
                       'output_loss_pct', 'fiscal_cost_pct', 'description']
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols].sort_values('start_year', ascending=False), 
                    use_container_width=True, hide_index=True)
        csv = df[available_cols].to_csv(index=False)
        st.download_button("📥 Download CSV", data=csv, file_name="economic_crises.csv", mime="text/csv")

st.markdown("---")

# Show data from unified_indicators
st.markdown("### 📈 Crisis Indicators from Research Datasets")
lv_rr_data = db.execute_query("""
    SELECT source, COUNT(*) as records, COUNT(DISTINCT indicator_code) as indicators,
           COUNT(DISTINCT country_iso3) as countries, MIN(year) as min_year, MAX(year) as max_year
    FROM unified_indicators 
    WHERE source IN ('LV', 'RR')
    GROUP BY source
""")
if lv_rr_data:
    lv_rr_df = pd.DataFrame(lv_rr_data)
    lv_rr_df['source'] = lv_rr_df['source'].map({'LV': 'Laeven-Valencia', 'RR': 'Reinhart-Rogoff'})
    st.dataframe(lv_rr_df, use_container_width=True, hide_index=True)
    st.info("👉 Use **Time Series** to explore these indicators")

st.markdown("---")
st.caption("Data sources: Laeven-Valencia (IMF), Reinhart-Rogoff")
