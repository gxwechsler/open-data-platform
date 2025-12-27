"""Unified Data Explorer - Browse all indicators from all sources."""
import streamlit as st
import pandas as pd
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ingestion.unified_data import UnifiedData

st.set_page_config(page_title="Data Explorer | Open Data Platform", page_icon="🔍", layout="wide")
st.title("🔍 Data Explorer")
st.markdown("Browse indicators from all data sources")

data = UnifiedData()

# Get summary stats
stats = data.get_summary_stats()
if stats:
    col1, col2, col3, col4, col5 = st.columns(5)
    with col1:
        st.metric("Total Records", f"{stats.get('total_records', 0):,}")
    with col2:
        st.metric("Sources", stats.get('sources', 0))
    with col3:
        st.metric("Countries", stats.get('countries', 0))
    with col4:
        st.metric("Indicators", stats.get('indicators', 0))
    with col5:
        st.metric("Years", f"{stats.get('min_year', '')}-{stats.get('max_year', '')}")

st.markdown("---")

# Sidebar filters
st.sidebar.header("Filters")

# Source filter
sources = data.get_sources()
selected_source = st.sidebar.selectbox("Data Source", ["All"] + sources)
source_filter = None if selected_source == "All" else selected_source

# Category filter
categories = data.get_categories(source=source_filter)
selected_category = st.sidebar.selectbox("Category", ["All"] + categories)
category_filter = None if selected_category == "All" else selected_category

# Search
search_term = st.sidebar.text_input("Search Indicators", placeholder="e.g., GDP, unemployment")

# Get indicators
if search_term:
    indicators = data.search_indicators(search_term)
else:
    indicators = data.get_indicators(source=source_filter, category=category_filter)

# Display indicators
st.markdown(f"### Available Indicators ({len(indicators)})")

if indicators:
    df = pd.DataFrame(indicators)
    
    # Make indicator_code clickable by storing selection
    if 'selected_indicator' not in st.session_state:
        st.session_state.selected_indicator = None
    
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
    
    # Indicator selection for preview
    st.markdown("---")
    st.markdown("### Preview Indicator Data")
    
    indicator_options = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in indicators}
    selected_indicator = st.selectbox(
        "Select indicator to preview",
        options=list(indicator_options.keys()),
        format_func=lambda x: indicator_options.get(x, x)
    )
    
    if selected_indicator:
        # Get countries
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
            preview_data = data.get_data(
                indicator_code=selected_indicator,
                countries=selected_countries,
                year_start=year_range[0],
                year_end=year_range[1]
            )
            
            if not preview_data.empty:
                # Pivot for display
                pivot_df = preview_data.pivot_table(
                    index='year', 
                    columns='country_name', 
                    values='value',
                    aggfunc='first'
                ).reset_index()
                
                st.dataframe(pivot_df, use_container_width=True, hide_index=True)
                
                # Download button
                csv = preview_data.to_csv(index=False)
                st.download_button(
                    "📥 Download CSV",
                    data=csv,
                    file_name=f"{selected_indicator}_data.csv",
                    mime="text/csv"
                )
            else:
                st.warning("No data found for selected filters.")
else:
    st.info("No indicators found. Try adjusting your filters or search term.")

# Footer
st.markdown("---")
st.caption("**Data Sources:** World Bank, FRED, IMF, OECD, UNHCR, UCDP, UNESCO, UNSD, IRENA")
