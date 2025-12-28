"""Unified Data Explorer - Browse all indicators from all sources."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ingestion.unified_data import UnifiedData

st.set_page_config(page_title="Data Explorer | Open Data Platform", page_icon="🔍", layout="wide")
st.title("🔍 Data Explorer")
st.markdown("Browse indicators from all data sources")

data = UnifiedData()

# Sidebar filters
st.sidebar.header("Filters")

# 1. Source filter
sources = data.get_sources()
selected_source = st.sidebar.selectbox("Data Source", ["All"] + sources)
source_filter = None if selected_source == "All" else selected_source

# 2. Category filter
categories = data.get_categories(source=source_filter)
selected_category = st.sidebar.selectbox("Category", ["All"] + categories)
category_filter = None if selected_category == "All" else selected_category

# 3. Indicator filter (based on source and category)
indicators = data.get_indicators(source=source_filter, category=category_filter)
indicator_options = {i['indicator_code']: i['indicator_name'] for i in indicators}
selected_indicator = st.sidebar.selectbox(
    "Indicator", 
    options=["All"] + list(indicator_options.keys()),
    format_func=lambda x: indicator_options.get(x, x) if x != "All" else "All"
)
indicator_filter = None if selected_indicator == "All" else selected_indicator

# Search (optional additional filter)
st.sidebar.markdown("---")
search_term = st.sidebar.text_input("Search Indicators", placeholder="e.g., GDP, unemployment")

# Apply search filter if provided
if search_term:
    indicators = data.search_indicators(search_term)
    indicator_options = {i['indicator_code']: i['indicator_name'] for i in indicators}

# Get summary stats
stats = data.get_summary_stats()

# Display filtered stats
filtered_indicators = len(indicators) if not indicator_filter else 1

col1, col2, col3, col4, col5 = st.columns(5)
with col1:
    st.metric("Total Records", f"{stats.get('total_records', 0):,}")
with col2:
    if source_filter:
        st.metric("Source", source_filter)
    else:
        st.metric("Sources", stats.get('sources', 0))
with col3:
    st.metric("Countries", stats.get('countries', 0))
with col4:
    st.metric("Indicators", filtered_indicators)
with col5:
    st.metric("Years", f"{stats.get('min_year', '')}-{stats.get('max_year', '')}")

st.markdown("---")

# If specific indicator selected, show its data
if indicator_filter:
    indicator_name = indicator_options.get(indicator_filter, indicator_filter)
    st.markdown(f"### {indicator_name}")
    
    # Country and year selection
    countries = data.get_countries(source=source_filter)
    country_options = {c['country_iso3']: c['country_name'] for c in countries}
    
    col1, col2 = st.columns(2)
    with col1:
        selected_countries = st.multiselect(
            "Select Countries",
            options=list(country_options.keys()),
            default=list(country_options.keys())[:5],
            format_func=lambda x: country_options.get(x, x)
        )
    with col2:
        min_year, max_year = data.get_year_range(source=source_filter)
        year_range = st.slider("Year Range", min_year, max_year, (min_year, max_year))
    
    if selected_countries:
        df = data.get_data(
            indicator_code=indicator_filter,
            countries=selected_countries,
            year_start=year_range[0],
            year_end=year_range[1]
        )
        
        if not df.empty:
            # Chart
            fig = px.line(
                df, x='year', y='value', color='country_name',
                title=f"{indicator_name} Over Time",
                markers=True
            )
            fig.update_layout(xaxis_title="Year", yaxis_title="Value", hovermode="x unified")
            st.plotly_chart(fig, use_container_width=True)
            
            # Data table (pivoted)
            st.markdown("### Data Table")
            pivot_df = df.pivot_table(
                index='year', 
                columns='country_name', 
                values='value',
                aggfunc='first'
            ).reset_index()
            
            st.dataframe(pivot_df, use_container_width=True, hide_index=True)
            
            # Download
            csv = df.to_csv(index=False)
            st.download_button(
                "📥 Download CSV",
                data=csv,
                file_name=f"{indicator_filter}_data.csv",
                mime="text/csv"
            )
        else:
            st.warning("No data found for selected filters.")
    else:
        st.info("Please select at least one country.")

else:
    # Show indicator list when no specific indicator selected
    st.markdown(f"### Available Indicators ({len(indicators)})")
    
    if indicators:
        df = pd.DataFrame(indicators)
        
        st.dataframe(
            df[['indicator_code', 'indicator_name', 'source', 'category']],
            use_container_width=True,
            hide_index=True,
            column_config={
                "indicator_code": "Code",
                "indicator_name": "Indicator Name",
                "source": "Source",
                "category": "Category"
            }
        )
        
        st.info("💡 Select an indicator from the sidebar dropdown to view its data and chart.")
    else:
        st.warning("No indicators found. Try adjusting your filters.")

# Footer
st.markdown("---")
st.caption("**Data Sources:** World Bank, FRED, IMF, OECD, UNHCR, UCDP, UNESCO, UNSD, IRENA")
