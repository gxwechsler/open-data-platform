"""World Bank Data page - Global development indicators."""
import streamlit as st
import pandas as pd
import plotly.express as px
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from database.connection import get_db_manager

st.set_page_config(page_title="World Bank Data | Open Data Platform", page_icon="🌍", layout="wide")
st.title("🌍 World Bank Data")
st.markdown("Development indicators for 44 countries")

db = get_db_manager()

# Initialize session state
if 'wb_indicator' not in st.session_state:
    st.session_state.wb_indicator = None
if 'wb_countries' not in st.session_state:
    st.session_state.wb_countries = ["USA", "CHN", "DEU", "JPN", "BRA"]
if 'wb_year_start' not in st.session_state:
    st.session_state.wb_year_start = 2000
if 'wb_year_end' not in st.session_state:
    st.session_state.wb_year_end = 2023

# Get available indicators
indicators_query = "SELECT DISTINCT indicator_code, indicator_name FROM worldbank_data ORDER BY indicator_name"
indicators_result = db.execute_query(indicators_query)
indicators_df = pd.DataFrame(indicators_result) if indicators_result else pd.DataFrame()

# Get available countries
countries_query = "SELECT DISTINCT country_iso3, country FROM worldbank_data ORDER BY country"
countries_result = db.execute_query(countries_query)
countries_df = pd.DataFrame(countries_result) if countries_result else pd.DataFrame()

if indicators_df.empty or countries_df.empty:
    st.warning("No World Bank data found. Please run the data loader first.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

indicator_options = dict(zip(indicators_df['indicator_code'], indicators_df['indicator_name']))

# Set default indicator
if st.session_state.wb_indicator is None or st.session_state.wb_indicator not in indicator_options:
    st.session_state.wb_indicator = list(indicator_options.keys())[0]

selected_indicator = st.sidebar.selectbox(
    "Select Indicator",
    options=list(indicator_options.keys()),
    index=list(indicator_options.keys()).index(st.session_state.wb_indicator) if st.session_state.wb_indicator in indicator_options else 0,
    format_func=lambda x: indicator_options.get(x, x)[:50],
    key="wb_indicator_select"
)
st.session_state.wb_indicator = selected_indicator

country_options = dict(zip(countries_df['country_iso3'], countries_df['country']))

# Filter valid countries
valid_countries = [c for c in st.session_state.wb_countries if c in country_options]
if not valid_countries:
    valid_countries = ["USA", "CHN", "DEU", "JPN", "BRA"]
    valid_countries = [c for c in valid_countries if c in country_options]

selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=list(country_options.keys()),
    default=valid_countries,
    format_func=lambda x: country_options.get(x, x),
    key="wb_countries_select"
)
st.session_state.wb_countries = selected_countries

# Get year range from data
year_query = "SELECT MIN(year) as min_year, MAX(year) as max_year FROM worldbank_data"
year_result = db.execute_query(year_query)
if year_result and year_result[0]['min_year']:
    data_min_year = int(year_result[0]['min_year'])
    data_max_year = int(year_result[0]['max_year'])
else:
    data_min_year, data_max_year = 1970, 2023

year_range = st.sidebar.slider(
    "Year Range",
    data_min_year, data_max_year,
    (max(data_min_year, st.session_state.wb_year_start), min(data_max_year, st.session_state.wb_year_end)),
    key="wb_year_slider"
)
st.session_state.wb_year_start = year_range[0]
st.session_state.wb_year_end = year_range[1]

# Fetch data using parameterized query
if selected_countries:
    # Build query with proper parameterization
    placeholders = ', '.join([f':country_{i}' for i in range(len(selected_countries))])
    query = f"""
        SELECT country_iso3, country, year, value 
        FROM worldbank_data 
        WHERE indicator_code = :indicator
        AND country_iso3 IN ({placeholders})
        AND year BETWEEN :year_start AND :year_end
        ORDER BY year, country
    """
    params = {
        'indicator': selected_indicator,
        'year_start': year_range[0],
        'year_end': year_range[1]
    }
    for i, country in enumerate(selected_countries):
        params[f'country_{i}'] = country
    
    result = db.execute_query(query, params)
    df = pd.DataFrame(result) if result else pd.DataFrame()
else:
    df = pd.DataFrame()

# Display
if not df.empty:
    indicator_name = indicator_options.get(selected_indicator, selected_indicator)
    st.markdown(f"### {indicator_name}")
    
    # Summary metrics
    col1, col2, col3, col4 = st.columns(4)
    latest_year = df['year'].max()
    latest_data = df[df['year'] == latest_year]
    
    with col1:
        st.metric("Latest Year", latest_year)
    with col2:
        st.metric("Countries", len(selected_countries))
    with col3:
        avg_val = latest_data['value'].mean()
        if pd.notna(avg_val):
            st.metric("Average", f"{avg_val:,.2f}")
        else:
            st.metric("Average", "N/A")
    with col4:
        total_records = len(df)
        st.metric("Data Points", total_records)
    
    # Tabs
    tab1, tab2, tab3 = st.tabs(["📈 Time Series", "🗺️ Map", "📋 Data"])
    
    with tab1:
        fig = px.line(
            df, x='year', y='value', color='country',
            title=f"{indicator_name} Over Time",
            markers=True
        )
        fig.update_layout(xaxis_title="Year", yaxis_title="Value", hovermode="x unified")
        st.plotly_chart(fig, use_container_width=True)
        
        # Bar chart for latest year
        if not latest_data.empty:
            st.markdown(f"#### Comparison ({latest_year})")
            fig2 = px.bar(
                latest_data.sort_values('value', ascending=True),
                x='value', y='country', orientation='h',
                title=f"{indicator_name} - {latest_year}"
            )
            st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        # World map
        if not latest_data.empty:
            map_data = latest_data.copy()
            fig = px.choropleth(
                map_data,
                locations='country_iso3',
                color='value',
                hover_name='country',
                color_continuous_scale="Viridis",
                title=f"{indicator_name} ({latest_year})"
            )
            fig.update_layout(geo=dict(showframe=False, projection_type='natural earth'), height=500)
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.info("No data available for map.")
    
    with tab3:
        st.dataframe(
            df.sort_values(['year', 'country'], ascending=[False, True]),
            use_container_width=True,
            hide_index=True
        )
        csv = df.to_csv(index=False)
        st.download_button("📥 Download CSV", data=csv, file_name=f"worldbank_{selected_indicator}.csv", mime="text/csv")

else:
    st.warning("No data found for the selected filters.")
    if not selected_countries:
        st.info("Please select at least one country.")

# Footer
st.markdown("---")
st.caption("**Data Source:** [World Bank Open Data](https://data.worldbank.org/)")
