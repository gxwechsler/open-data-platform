"""Unified Time Series - Multi-indicator comparison with dual y-axes."""
import streamlit as st
import pandas as pd
import plotly.graph_objects as go
from plotly.subplots import make_subplots
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

# 1. Source filter (multiselect)
sources = data.get_sources()
selected_sources = st.sidebar.multiselect(
    "Data Sources",
    options=sources,
    default=sources
)

if not selected_sources:
    st.warning("Please select at least one data source.")
    st.stop()

# 2. Category filter (multiselect)
all_categories = []
for source in selected_sources:
    cats = data.get_categories(source=source)
    all_categories.extend(cats)
all_categories = sorted(list(set(all_categories)))

selected_categories = st.sidebar.multiselect(
    "Categories",
    options=all_categories,
    default=all_categories[:3] if len(all_categories) > 3 else all_categories
)

if not selected_categories:
    st.warning("Please select at least one category.")
    st.stop()

# 3. Get all indicators for selected sources and categories
all_indicators = []
for source in selected_sources:
    for category in selected_categories:
        indicators = data.get_indicators(source=source, category=category)
        all_indicators.extend(indicators)

# Remove duplicates by indicator_code
seen = set()
unique_indicators = []
for ind in all_indicators:
    if ind['indicator_code'] not in seen:
        seen.add(ind['indicator_code'])
        unique_indicators.append(ind)

if not unique_indicators:
    st.warning("No indicators found for selected filters.")
    st.stop()

indicator_options = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in unique_indicators}

# 4. Indicator selection (multiselect - up to 4)
selected_indicators = st.sidebar.multiselect(
    "Select Indicators (max 4)",
    options=list(indicator_options.keys()),
    default=[list(indicator_options.keys())[0]] if indicator_options else [],
    format_func=lambda x: indicator_options.get(x, x)[:50],
    max_selections=4
)

if not selected_indicators:
    st.info("Please select at least one indicator from the sidebar.")
    st.stop()

# 5. Countries
countries = data.get_countries()
country_options = {c['country_iso3']: c['country_name'] for c in countries}

default_countries = []
for code in ["USA", "CHN", "DEU", "JPN", "BRA"]:
    if code in country_options:
        default_countries.append(code)

selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=list(country_options.keys()),
    default=default_countries[:5],
    format_func=lambda x: country_options.get(x, x)
)

if not selected_countries:
    st.warning("Please select at least one country.")
    st.stop()

# 6. Year range
min_year, max_year = data.get_year_range()
year_range = st.sidebar.slider("Year Range", min_year, max_year, (max(min_year, 1990), max_year))

# 7. Chart options
st.sidebar.markdown("---")
st.sidebar.subheader("Chart Options")
normalize = st.sidebar.checkbox("Normalize (Index to 100)", value=False)
show_markers = st.sidebar.checkbox("Show Markers", value=True)
use_dual_axis = st.sidebar.checkbox("Use Dual Y-Axis", value=len(selected_indicators) > 1)

# Fetch data for all selected indicators
all_data = []
for ind_code in selected_indicators:
    df = data.get_data(
        indicator_code=ind_code,
        countries=selected_countries,
        year_start=year_range[0],
        year_end=year_range[1]
    )
    if not df.empty:
        all_data.append(df)

if not all_data:
    st.warning("No data found for selected filters.")
    st.stop()

combined_df = pd.concat(all_data, ignore_index=True)

# Create chart
if len(selected_indicators) == 1:
    # Single indicator - simple line chart
    ind_name = indicator_options.get(selected_indicators[0], selected_indicators[0])
    st.markdown(f"### {ind_name}")
    
    df = all_data[0]
    
    if normalize:
        for country in df['country_iso3'].unique():
            mask = df['country_iso3'] == country
            first_val = df.loc[mask].sort_values('year')['value'].iloc[0]
            if first_val and first_val != 0:
                df.loc[mask, 'value'] = (df.loc[mask, 'value'] / first_val) * 100
    
    fig = go.Figure()
    for country in df['country_name'].unique():
        country_df = df[df['country_name'] == country].sort_values('year')
        fig.add_trace(go.Scatter(
            x=country_df['year'],
            y=country_df['value'],
            mode='lines+markers' if show_markers else 'lines',
            name=country
        ))
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Index (Base=100)" if normalize else "Value",
        hovermode="x unified",
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

else:
    # Multiple indicators
    st.markdown("### Multi-Indicator Comparison")
    
    if len(selected_countries) == 1:
        # One country, multiple indicators
        country_name = country_options.get(selected_countries[0], selected_countries[0])
        st.markdown(f"**Country:** {country_name}")
        
        if use_dual_axis and len(selected_indicators) >= 2:
            # Dual y-axis
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            
            for i, ind_code in enumerate(selected_indicators):
                ind_df = combined_df[combined_df['indicator_code'] == ind_code].sort_values('year')
                ind_name = indicator_options.get(ind_code, ind_code)[:40]
                
                if normalize and not ind_df.empty:
                    first_val = ind_df['value'].iloc[0]
                    if first_val and first_val != 0:
                        ind_df = ind_df.copy()
                        ind_df['value'] = (ind_df['value'] / first_val) * 100
                
                secondary = i >= len(selected_indicators) // 2 and len(selected_indicators) > 1
                
                fig.add_trace(
                    go.Scatter(
                        x=ind_df['year'],
                        y=ind_df['value'],
                        mode='lines+markers' if show_markers else 'lines',
                        name=ind_name,
                        line=dict(color=colors[i % len(colors)])
                    ),
                    secondary_y=secondary
                )
            
            fig.update_layout(
                xaxis_title="Year",
                hovermode="x unified",
                height=500
            )
            fig.update_yaxes(title_text="Left Axis", secondary_y=False)
            fig.update_yaxes(title_text="Right Axis", secondary_y=True)
            
        else:
            # Single y-axis (normalized recommended)
            fig = go.Figure()
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            
            for i, ind_code in enumerate(selected_indicators):
                ind_df = combined_df[combined_df['indicator_code'] == ind_code].sort_values('year')
                ind_name = indicator_options.get(ind_code, ind_code)[:40]
                
                if normalize and not ind_df.empty:
                    first_val = ind_df['value'].iloc[0]
                    if first_val and first_val != 0:
                        ind_df = ind_df.copy()
                        ind_df['value'] = (ind_df['value'] / first_val) * 100
                
                fig.add_trace(go.Scatter(
                    x=ind_df['year'],
                    y=ind_df['value'],
                    mode='lines+markers' if show_markers else 'lines',
                    name=ind_name,
                    line=dict(color=colors[i % len(colors)])
                ))
            
            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Index (Base=100)" if normalize else "Value",
                hovermode="x unified",
                height=500
            )
        
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        # Multiple countries, multiple indicators - use tabs
        tab_labels = [indicator_options.get(ind, ind)[:30] for ind in selected_indicators]
        tabs = st.tabs(tab_labels)
        
        for idx, (tab, ind_code) in enumerate(zip(tabs, selected_indicators)):
            with tab:
                ind_df = combined_df[combined_df['indicator_code'] == ind_code]
                ind_name = indicator_options.get(ind_code, ind_code)
                
                if normalize and not ind_df.empty:
                    ind_df = ind_df.copy()
                    for country in ind_df['country_iso3'].unique():
                        mask = ind_df['country_iso3'] == country
                        first_val = ind_df.loc[mask].sort_values('year')['value'].iloc[0]
                        if first_val and first_val != 0:
                            ind_df.loc[mask, 'value'] = (ind_df.loc[mask, 'value'] / first_val) * 100
                
                fig = go.Figure()
                for country in ind_df['country_name'].unique():
                    country_df = ind_df[ind_df['country_name'] == country].sort_values('year')
                    fig.add_trace(go.Scatter(
                        x=country_df['year'],
                        y=country_df['value'],
                        mode='lines+markers' if show_markers else 'lines',
                        name=country
                    ))
                
                fig.update_layout(
                    title=ind_name,
                    xaxis_title="Year",
                    yaxis_title="Index (Base=100)" if normalize else "Value",
                    hovermode="x unified",
                    height=450
                )
                st.plotly_chart(fig, use_container_width=True)

# Summary statistics
st.markdown("---")
st.markdown("### Summary Statistics")

for ind_code in selected_indicators:
    ind_df = combined_df[combined_df['indicator_code'] == ind_code]
    ind_name = indicator_options.get(ind_code, ind_code)
    
    if not ind_df.empty:
        with st.expander(f"📊 {ind_name[:50]}"):
            latest_year = ind_df['year'].max()
            latest_data = ind_df[ind_df['year'] == latest_year][['country_name', 'value', 'year']].sort_values('value', ascending=False)
            latest_data.columns = ['Country', 'Value', 'Year']
            st.dataframe(latest_data, use_container_width=True, hide_index=True)

# Data download
st.markdown("---")
csv = combined_df.to_csv(index=False)
st.download_button(
    "📥 Download All Data (CSV)",
    data=csv,
    file_name="timeseries_data.csv",
    mime="text/csv"
)

# Footer
st.markdown("---")
st.caption("**Data Sources:** World Bank, IMF, FRED, OECD, UNHCR, UCDP, UNESCO, UNSD, IRENA")
