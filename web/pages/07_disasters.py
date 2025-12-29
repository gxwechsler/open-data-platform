"""Disasters - Natural disaster events database."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="Disasters | Open Data Platform", page_icon="🌪️", layout="wide")
st.title("🌪️ Natural Disasters Database")

db = get_db_manager()

COUNTRY_NAMES = {
    "CHN": "China", "JPN": "Japan", "USA": "United States", "TUR": "Turkey",
    "IRN": "Iran", "IND": "India", "ITA": "Italy", "MEX": "Mexico", "CHL": "Chile",
    "NZL": "New Zealand", "COL": "Colombia", "DZA": "Algeria", "MAR": "Morocco",
    "DEU": "Germany", "GBR": "United Kingdom", "AUS": "Australia", "BRA": "Brazil",
    "NER": "Niger", "ETH": "Ethiopia", "GHA": "Ghana", "VNM": "Vietnam",
    "FRA": "France", "ESP": "Spain", "NLD": "Netherlands", "CAN": "Canada",
    "ZAF": "South Africa", "PRT": "Portugal", "COD": "DR Congo", "HTI": "Haiti",
    "PAK": "Pakistan", "BGD": "Bangladesh", "PHL": "Philippines", "IDN": "Indonesia",
    "MMR": "Myanmar", "THA": "Thailand", "KOR": "South Korea"
}

# Session state
for key, val in [('dis_type', 'All'), ('dis_group', 'All'), ('dis_country', 'All'),
                 ('dis_yr_s', 1976), ('dis_yr_e', 2024)]:
    if key not in st.session_state:
        st.session_state[key] = val

@st.cache_data(ttl=300)
def get_types():
    r = db.execute_query("SELECT DISTINCT disaster_type FROM event_level_unified_data WHERE disaster_type IS NOT NULL ORDER BY disaster_type")
    return [x['disaster_type'] for x in r] if r else []

@st.cache_data(ttl=300)
def get_groups():
    r = db.execute_query("SELECT DISTINCT disaster_group FROM event_level_unified_data WHERE disaster_group IS NOT NULL ORDER BY disaster_group")
    return [x['disaster_group'] for x in r] if r else []

@st.cache_data(ttl=300)
def get_countries():
    r = db.execute_query("SELECT DISTINCT country_iso3 FROM event_level_unified_data WHERE country_iso3 IS NOT NULL ORDER BY country_iso3")
    return [x['country_iso3'] for x in r] if r else []

@st.cache_data(ttl=300)
def get_years():
    r = db.execute_query("SELECT MIN(year) as mn, MAX(year) as mx FROM event_level_unified_data")
    return (int(r[0]['mn']), int(r[0]['mx'])) if r and r[0]['mn'] else (1976, 2024)

def get_data(dtype=None, dgroup=None, country=None, yr_s=None, yr_e=None):
    q = "SELECT * FROM event_level_unified_data WHERE 1=1"
    p = {}
    if dtype:
        q += " AND disaster_type = :dt"
        p['dt'] = dtype
    if dgroup:
        q += " AND disaster_group = :dg"
        p['dg'] = dgroup
    if country:
        q += " AND country_iso3 = :co"
        p['co'] = country
    if yr_s:
        q += " AND year >= :ys"
        p['ys'] = yr_s
    if yr_e:
        q += " AND year <= :ye"
        p['ye'] = yr_e
    q += " ORDER BY year DESC, deaths DESC NULLS LAST"
    r = db.execute_query(q, p or None)
    if r:
        df = pd.DataFrame(r)
        df['country'] = df['country_iso3'].map(lambda x: COUNTRY_NAMES.get(x, x))
        return df
    return pd.DataFrame()

st.sidebar.header("Filters")

types = get_types()
opts = ["All"] + types
idx = opts.index(st.session_state.dis_type) if st.session_state.dis_type in opts else 0
dtype = st.sidebar.selectbox("Type", opts, index=idx, key="dis_type_sel")
st.session_state.dis_type = dtype

groups = get_groups()
opts = ["All"] + groups
idx = opts.index(st.session_state.dis_group) if st.session_state.dis_group in opts else 0
dgroup = st.sidebar.selectbox("Group", opts, index=idx, key="dis_grp_sel")
st.session_state.dis_group = dgroup

mn, mx = get_years()
yr = st.sidebar.slider("Years", mn, mx, (st.session_state.dis_yr_s, st.session_state.dis_yr_e), key="dis_yr")
st.session_state.dis_yr_s, st.session_state.dis_yr_e = yr

countries = get_countries()
opts = ["All"] + countries
idx = opts.index(st.session_state.dis_country) if st.session_state.dis_country in opts else 0
country = st.sidebar.selectbox("Country", opts, index=idx,
    format_func=lambda x: COUNTRY_NAMES.get(x, x) if x != "All" else "All", key="dis_co_sel")
st.session_state.dis_country = country

df = get_data(
    dtype if dtype != "All" else None,
    dgroup if dgroup != "All" else None,
    country if country != "All" else None,
    yr[0], yr[1]
)

if not df.empty:
    col1, col2, col3, col4 = st.columns(4)
    col1.metric("Events", len(df))
    deaths = df['deaths'].sum() if 'deaths' in df.columns else 0
    col2.metric("Deaths", f"{int(deaths):,}" if pd.notna(deaths) else "N/A")
    affected = df['total_affected'].sum() if 'total_affected' in df.columns else 0
    col3.metric("Affected", f"{int(affected):,}" if pd.notna(affected) else "N/A")
    damage = df['damage_usd'].sum() if 'damage_usd' in df.columns else 0
    col4.metric("Damage", f"${float(damage)/1e9:,.1f}B" if pd.notna(damage) and damage > 0 else "N/A")

    tab1, tab2, tab3 = st.tabs(["📊 Charts", "📈 Trends", "📋 Data"])

    with tab1:
        col1, col2 = st.columns(2)
        with col1:
            tc = df['disaster_type'].value_counts().reset_index()
            tc.columns = ['Type', 'Count']
            fig = px.pie(tc, values='Count', names='Type', title="By Type")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            if 'deaths' in df.columns:
                dbt = df.groupby('disaster_type')['deaths'].sum().reset_index().sort_values('deaths', ascending=True)
                fig = px.bar(dbt, x='deaths', y='disaster_type', orientation='h', title="Deaths by Type")
                st.plotly_chart(fig, use_container_width=True)
        
        st.markdown("#### Deadliest Events")
        if 'deaths' in df.columns:
            cols = ['year', 'country', 'disaster_type', 'event_name', 'deaths']
            avail = [c for c in cols if c in df.columns]
            st.dataframe(df.nlargest(10, 'deaths')[avail], use_container_width=True, hide_index=True)

    with tab2:
        yearly = df.groupby('year').agg({'id': 'count', 'deaths': 'sum', 'total_affected': 'sum'}).reset_index()
        yearly.columns = ['year', 'events', 'deaths', 'affected']
        if not yearly.empty:
            col1, col2 = st.columns(2)
            with col1:
                fig = px.bar(yearly, x='year', y='events', title="Events by Year")
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig = px.line(yearly, x='year', y='deaths', title="Deaths by Year", markers=True)
                st.plotly_chart(fig, use_container_width=True)
            if 'damage_usd' in df.columns:
                dmg = df.groupby('year')['damage_usd'].sum().reset_index()
                dmg['damage_b'] = dmg['damage_usd'].astype(float) / 1e9
                fig = px.bar(dmg, x='year', y='damage_b', title="Damage by Year ($B)")
                st.plotly_chart(fig, use_container_width=True)

    with tab3:
        cols = ['year', 'country', 'disaster_type', 'disaster_group', 'event_name', 'deaths', 'total_affected', 'damage_usd']
        avail = [c for c in cols if c in df.columns]
        st.dataframe(df[avail], use_container_width=True, hide_index=True)
        st.download_button("📥 CSV", df[avail].to_csv(index=False), "disasters.csv", "text/csv")
else:
    st.warning("No disaster data found.")

st.markdown("---")
st.caption("Source: EM-DAT (CRED/UCLouvain)")
