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

# Get available indicators
indicators_query = "SELECT DISTINCT indicator_code, indicator_name FROM worldbank_data ORDER BY indicator_name"
indicators_df = pd.DataFrame(db.execute_query(indicators_query))

# Get available countries
countries_query = "SELECT DISTINCT country_iso3, country FROM worldbank_data ORDER BY country"
countries_df = pd.DataFrame(db.execute_query(countries_query))

if indicators_df.empty or countries_df.empty:
    st.warning("No World Bank data found. Please run the data loader first.")
    st.stop()

# Sidebar filters
st.sidebar.header("Filters")

indicator_options = dict(zip(indicators_df['indicator_code'], indicators_df['indicator_name']))
selected_indicator = st.sidebar.selectbox(
    "Select Indicator",
    options=list(indicator_options.keys()),
    format_func=lambda x: indicator_options.get(x, x)[:50]
)

country_options = dict(zip(countries_df['country_iso3'], countries_df['country']))
selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=list(country_options.keys()),
    default=["USA", "CHN", "DEU", "JPN", "BRA"],
    format_func=lambda x: country_options.get(x, x)
)

year_range = st.sidebar.slider("Year Range", 2000, 2023, (2000, 2023))

# Fetch data
if selected_countries:
    countries_str = "','".join(selected_countries)
    query = f"""
        SELECT country_iso3, country, year, value 
        FROM worldbank_data 
        WHERE indicator_code = '{selected_indicator}'
        AND country_iso3 IN ('{countries_str}')
        AND year BETWEEN {year_range[0]} AND {year_range[1]}
        ORDER BY year, country
    """
    df = pd.DataFrame(db.execute_query(query))
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
        st.metric("Average", f"{avg_val:,.2f}")
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
        st.markdown(f"#### Comparison ({latest_year})")
        fig2 = px.bar(
            latest_data.sort_values('value', ascending=True),
            x='value', y='country', orientation='h',
            title=f"{indicator_name} - {latest_year}"
        )
        st.plotly_chart(fig2, use_container_width=True)
    
    with tab2:
        # World map
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

# Footer
st.markdown("---")
st.caption("**Data Source:** [World Bank Open Data](https://data.worldbank.org/)")
# World Bank Data
