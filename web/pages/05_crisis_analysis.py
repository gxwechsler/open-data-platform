"""Crisis Analysis page - Twin crises and contagion."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager
from ingestion.reinhart_rogoff import CrisisData

st.set_page_config(page_title="Crisis Analysis | Open Data Platform", page_icon="📊", layout="wide")
st.title("📊 Crisis Analysis")

db = get_db_manager()
crises = CrisisData(db)

tab1, tab2, tab3 = st.tabs(["👯 Twin Crises", "🔗 Contagion Patterns", "📈 Severity Analysis"])

with tab1:
    st.markdown("### Twin Crises Analysis")
    st.markdown("**Twin crises** occur when banking and currency crises happen simultaneously.")
    twins = crises.get_twin_crises()
    if not twins.empty:
        col1, col2 = st.columns([2, 1])
        with col1:
            display_df = twins[['start_year', 'country', 'banking_description', 'currency_description']].copy()
            display_df.columns = ['Year', 'Country', 'Banking Crisis', 'Currency Crisis']
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        with col2:
            st.metric("Twin Crisis Events", len(twins))
            st.metric("Countries Affected", twins['country_iso3'].nunique())
    else:
        st.info("No twin crises found in the dataset.")

with tab2:
    st.markdown("### Contagion Analysis")
    df = crises.get_data()
    if not df.empty:
        year_to_analyze = st.selectbox("Select Year", sorted(df['start_year'].unique(), reverse=True))
        year_crises = df[df['start_year'] == year_to_analyze]
        if not year_crises.empty:
            st.dataframe(year_crises[['country', 'crisis_type', 'description']], use_container_width=True, hide_index=True)
        else:
            st.info(f"No crises in {year_to_analyze}")

with tab3:
    st.markdown("### Severity Analysis")
    df = crises.get_data()
    if not df.empty and 'output_loss_pct' in df.columns:
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("#### Most Severe by Output Loss")
            severe = df.dropna(subset=['output_loss_pct']).nlargest(10, 'output_loss_pct')
            if not severe.empty:
                severe['label'] = severe['country'] + ' (' + severe['start_year'].astype(str) + ')'
                fig = px.bar(severe, x='output_loss_pct', y='label', orientation='h', color='crisis_type')
                fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                st.plotly_chart(fig, use_container_width=True)
        with col2:
            if 'fiscal_cost_pct' in df.columns:
                st.markdown("#### Most Severe by Fiscal Cost")
                severe = df.dropna(subset=['fiscal_cost_pct']).nlargest(10, 'fiscal_cost_pct')
                if not severe.empty:
                    severe['label'] = severe['country'] + ' (' + severe['start_year'].astype(str) + ')'
                    fig = px.bar(severe, x='fiscal_cost_pct', y='label', orientation='h', color='crisis_type')
                    fig.update_layout(yaxis={'categoryorder': 'total ascending'})
                    st.plotly_chart(fig, use_container_width=True)
