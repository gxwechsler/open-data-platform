"""Unified Time Series - Chart any indicator from any source."""
import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from ingestion.unified_data import UnifiedData

st.set_page_config(page_title="Time Series | Open Data Platform", page_icon="📈", layout="wide")
st.title("📈 Time Series Analysis")
st.markdown("Chart and compare indicators across countries and sources")

data = UnifiedData()

# Sidebar
st.sidebar.header("Configuration")

# Source filter
sources = data.get_sources()
selected_source = st.sidebar.selectbox("Data Source", ["All"] + sources)
source_filter = None if selected_source == "All" else selected_source

# Category filter (NEW)
categories = data.get_categories(source=source_filter)
selected_category = st.sidebar.selectbox("Category", ["All"] + categories)
category_filter = None if selected_category == "All" else selected_category

# Get indicators filtered by source AND category
indicators = data.get_indicators(source=source_filter, category=category_filter)
if not indicators:
    st.warning("No indicators available for selected filters.")
    st.stop()

indicator_options = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in indicators}

# Indicator selection
selected_indicator = st.sidebar.selectbox(
    "Select Indicator",
    options=list(indicator_options.keys()),
    format_func=lambda x: indicator_options.get(x, x)[:60]
)

# Countries
countries = data.get_countries(source=source_filter)
country_options = {c['country_iso3']: c['country_name'] for c in countries}

selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=list(country_options.keys()),
    default=["USA", "CHN", "DEU", "JPN", "BRA"][:min(5, len(country_options))],
    format_func=lambda x: country_options.get(x, x)
)

# Year range
min_year, max_year = data.get_year_range(source=source_filter)
year_range = st.sidebar.slider("Year Range", min_year, max_year, (min_year, max_year))

# Chart options
st.sidebar.markdown("---")
st.sidebar.subheader("Chart Options")
chart_type = st.sidebar.radio("Chart Type", ["Line", "Bar", "Area"])
show_markers = st.sidebar.checkbox("Show Markers", value=True)
normalize = st.sidebar.checkbox("Normalize (Index to 100)", value=False)

# Get data
if selected_indicator and selected_countries:
    df = data.get_data(
        indicator_code=selected_indicator,
        countries=selected_countries,
        year_start=year_range[0],
        year_end=year_range[1]
    )
    
    if not df.empty:
        # Get indicator name for title
        indicator_name = indicator_options.get(selected_indicator, selected_indicator)
        
        # Normalize if requested
        if normalize:
            # Index to first year = 100
            for country in df['country_iso3'].unique():
                mask = df['country_iso3'] == country
                first_value = df.loc[mask, 'value'].iloc[0]
                if first_value and first_value != 0:
                    df.loc[mask, 'value'] = (df.loc[mask, 'value'] / first_value) * 100
            indicator_name += " (Indexed to 100)"
        
        # Create chart
        st.markdown(f"### {indicator_name}")
        
        if chart_type == "Line":
            fig = px.line(
                df, x='year', y='value', color='country_name',
                markers=show_markers
            )
        elif chart_type == "Bar":
            fig = px.bar(
                df, x='year', y='value', color='country_name',
                barmode='group'
            )
        else:  # Area
            fig = px.area(
                df, x='year', y='value', color='country_name'
            )
        
        fig.update_layout(
            xaxis_title="Year",
            yaxis_title="Value",
            hovermode="x unified",
            legend_title="Country",
            height=500
        )
        
        st.plotly_chart(fig, use_container_width=True)
        
        # Summary stats
        st.markdown("### Summary Statistics")
        col1, col2 = st.columns(2)
        
        with col1:
            # Latest values
            latest_year = df['year'].max()
            latest_data = df[df['year'] == latest_year][['country_name', 'value']].sort_values('value', ascending=False)
            latest_data.columns = ['Country', f'Value ({latest_year})']
            st.markdown(f"**Latest Values ({latest_year})**")
            st.dataframe(latest_data, use_container_width=True, hide_index=True)
        
        with col2:
            # Growth rates
            st.markdown("**Growth (First to Last Year)**")
            growth_data = []
            for country in df['country_name'].unique():
                country_df = df[df['country_name'] == country].sort_values('year')
                if len(country_df) >= 2:
                    first_val = country_df['value'].iloc[0]
                    last_val = country_df['value'].iloc[-1]
                    if first_val and first_val != 0:
                        growth = ((last_val - first_val) / first_val) * 100
                        growth_data.append({'Country': country, 'Growth %': f"{growth:.1f}%"})
            if growth_data:
                st.dataframe(pd.DataFrame(growth_data), use_container_width=True, hide_index=True)
        
        # Data table
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
            file_name=f"{selected_indicator}_timeseries.csv",
            mime="text/csv"
        )
    else:
        st.warning("No data found for selected filters.")
else:
    st.info("Please select an indicator and at least one country.")

# Multi-indicator comparison
st.markdown("---")
st.markdown("### Compare Multiple Indicators")

with st.expander("Compare different indicators for one country"):
    compare_country = st.selectbox(
        "Select Country for Comparison",
        options=list(country_options.keys()),
        format_func=lambda x: country_options.get(x, x),
        key="compare_country"
    )
    
    compare_indicators = st.multiselect(
        "Select Indicators to Compare",
        options=list(indicator_options.keys()),
        format_func=lambda x: indicator_options.get(x, x)[:50],
        key="compare_indicators"
    )
    
    if compare_country and compare_indicators:
        compare_df = data.get_multi_indicator_data(
            indicator_codes=compare_indicators,
            countries=[compare_country],
            year_start=year_range[0],
            year_end=year_range[1]
        )
        
        if not compare_df.empty:
            # Normalize for comparison
            fig = go.Figure()
            for indicator in compare_indicators:
                ind_df = compare_df[compare_df['indicator_code'] == indicator].sort_values('year')
                if not ind_df.empty:
                    # Normalize to first value = 100
                    first_val = ind_df['value'].iloc[0]
                    if first_val and first_val != 0:
                        normalized = (ind_df['value'] / first_val) * 100
                    else:
                        normalized = ind_df['value']
                    
                    ind_name = indicator_options.get(indicator, indicator)[:40]
                    fig.add_trace(go.Scatter(
                        x=ind_df['year'],
                        y=normalized,
                        mode='lines+markers',
                        name=ind_name
                    ))
            
            fig.update_layout(
                title=f"Indicator Comparison - {country_options.get(compare_country, compare_country)} (Indexed to 100)",
                xaxis_title="Year",
                yaxis_title="Index (First Year = 100)",
                hovermode="x unified",
                height=500
            )
            st.plotly_chart(fig, use_container_width=True)

# Footer
st.markdown("---")
st.caption("**Data Sources:** World Bank, FRED, IMF, OECD, UNHCR, UCDP, UNESCO, UNSD, IRENA")
