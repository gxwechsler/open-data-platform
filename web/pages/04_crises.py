"""Crises page - Financial crises database."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager
from ingestion.reinhart_rogoff import CrisisData

st.set_page_config(page_title="Crises | Open Data Platform", page_icon="🏦", layout="wide")
st.title("🏦 Financial Crises Database")

db = get_db_manager()
crises = CrisisData(db)

st.sidebar.header("Filters")
crisis_type = st.sidebar.selectbox("Crisis Type", ["All"] + crises.get_crisis_types())
source = st.sidebar.selectbox("Data Source", ["All"] + crises.get_sources())
year_range = st.sidebar.slider("Year Range", 1800, 2024, (1980, 2024))

df = crises.get_data(
    crisis_type=crisis_type if crisis_type != "All" else None,
    source=source if source != "All" else None,
    year_start=year_range[0], year_end=year_range[1]
)

if df.empty:
    st.warning("No crises found with the current filters.")
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
        fig = px.scatter(df, x='start_year', y='country', color='crisis_type', title="Financial Crises Timeline")
        fig.update_traces(marker=dict(size=12))
        st.plotly_chart(fig, use_container_width=True)
        
        summary = crises.get_summary_by_decade()
        if not summary.empty:
            fig = px.bar(summary, x='decade', y='count', title="Crises by Decade")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab2:
        col1, col2 = st.columns(2)
        with col1:
            type_counts = df['crisis_type'].value_counts().reset_index()
            type_counts.columns = ['Crisis Type', 'Count']
            fig = px.pie(type_counts, values='Count', names='Crisis Type', title="By Type")
            st.plotly_chart(fig, use_container_width=True)
        with col2:
            source_counts = df['source'].value_counts().reset_index()
            source_counts.columns = ['Source', 'Count']
            fig = px.pie(source_counts, values='Count', names='Source', title="By Source")
            st.plotly_chart(fig, use_container_width=True)
    
    with tab3:
        display_cols = ['start_year', 'end_year', 'country', 'crisis_type', 'source', 'description']
        available_cols = [c for c in display_cols if c in df.columns]
        st.dataframe(df[available_cols].sort_values('start_year', ascending=False), use_container_width=True, hide_index=True)
        csv = df.to_csv(index=False)
        st.download_button("📥 Download CSV", data=csv, file_name="financial_crises.csv", mime="text/csv")
