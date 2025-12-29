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

# Initialize session state
if 'ts_sources' not in st.session_state:
    st.session_state.ts_sources = None
if 'ts_categories' not in st.session_state:
    st.session_state.ts_categories = None
if 'ts_indicators' not in st.session_state:
    st.session_state.ts_indicators = None
if 'ts_countries' not in st.session_state:
    st.session_state.ts_countries = None
if 'ts_year_start' not in st.session_state:
    st.session_state.ts_year_start = 1990
if 'ts_year_end' not in st.session_state:
    st.session_state.ts_year_end = 2024
if 'ts_normalize' not in st.session_state:
    st.session_state.ts_normalize = False
if 'ts_markers' not in st.session_state:
    st.session_state.ts_markers = True
if 'ts_dual_axis' not in st.session_state:
    st.session_state.ts_dual_axis = False

# Sidebar
st.sidebar.header("Configuration")

# 1. Source filter
sources = data.get_sources()
if st.session_state.ts_sources is None:
    st.session_state.ts_sources = sources
valid_sources = [s for s in st.session_state.ts_sources if s in sources]
if not valid_sources:
    valid_sources = sources

selected_sources = st.sidebar.multiselect(
    "Data Sources",
    options=sources,
    default=valid_sources,
    key="ts_sources_select"
)
st.session_state.ts_sources = selected_sources

if not selected_sources:
    st.warning("Please select at least one data source.")
    st.stop()

# 2. Category filter
all_categories = []
for source in selected_sources:
    cats = data.get_categories(source=source)
    all_categories.extend(cats)
all_categories = sorted(list(set(all_categories)))

if st.session_state.ts_categories is None:
    st.session_state.ts_categories = all_categories[:3] if len(all_categories) > 3 else all_categories
valid_categories = [c for c in st.session_state.ts_categories if c in all_categories]
if not valid_categories:
    valid_categories = all_categories[:3] if len(all_categories) > 3 else all_categories

selected_categories = st.sidebar.multiselect(
    "Categories",
    options=all_categories,
    default=valid_categories,
    key="ts_categories_select"
)
st.session_state.ts_categories = selected_categories

if not selected_categories:
    st.warning("Please select at least one category.")
    st.stop()

# 3. Get all indicators
all_indicators = []
for source in selected_sources:
    for category in selected_categories:
        indicators = data.get_indicators(source=source, category=category)
        all_indicators.extend(indicators)

seen = set()
unique_indicators = []
for ind in all_indicators:
    if ind['indicator_code'] not in seen:
        seen.add(ind['indicator_code'])
        unique_indicators.append(ind)

if not unique_indicators:
    st.warning("No indicators found for selected filters.")
    st.stop()

# Build indicator options - FULL names, no truncation
indicator_options = {i['indicator_code']: f"{i['indicator_name']} ({i['source']})" for i in unique_indicators}
indicator_units = {i['indicator_code']: i.get('units', '') for i in unique_indicators}

if st.session_state.ts_indicators is None:
    st.session_state.ts_indicators = [list(indicator_options.keys())[0]] if indicator_options else []
valid_indicators = [i for i in st.session_state.ts_indicators if i in indicator_options]
if not valid_indicators and indicator_options:
    valid_indicators = [list(indicator_options.keys())[0]]

# 4. Indicator selection - SHOW FULL NAME (no truncation)
selected_indicators = st.sidebar.multiselect(
    "Select Indicators (max 4)",
    options=list(indicator_options.keys()),
    default=valid_indicators,
    format_func=lambda x: indicator_options.get(x, x),  # Full name, no [:50] truncation
    max_selections=4,
    key="ts_indicators_select"
)
st.session_state.ts_indicators = selected_indicators

if not selected_indicators:
    st.info("Please select at least one indicator from the sidebar.")
    st.stop()

# Show selected indicators for reference
st.sidebar.markdown("---")
st.sidebar.markdown("**Selected:**")
for ind in selected_indicators:
    st.sidebar.caption(f"• {indicator_options.get(ind, ind)}")

# 5. Countries
countries = data.get_countries()
country_options = {c['country_iso3']: c['country_name'] for c in countries}

if st.session_state.ts_countries is None:
    default_countries = []
    for code in ["USA", "CHN", "DEU", "JPN", "BRA"]:
        if code in country_options:
            default_countries.append(code)
    st.session_state.ts_countries = default_countries[:5]

valid_countries = [c for c in st.session_state.ts_countries if c in country_options]
if not valid_countries:
    for code in ["USA", "CHN", "DEU", "JPN", "BRA"]:
        if code in country_options:
            valid_countries.append(code)
    valid_countries = valid_countries[:5]

selected_countries = st.sidebar.multiselect(
    "Select Countries",
    options=list(country_options.keys()),
    default=valid_countries,
    format_func=lambda x: country_options.get(x, x),
    key="ts_countries_select"
)
st.session_state.ts_countries = selected_countries

if not selected_countries:
    st.warning("Please select at least one country.")
    st.stop()

# 6. Year range
min_year, max_year = data.get_year_range()
year_range = st.sidebar.slider(
    "Year Range", 
    min_year, max_year, 
    (max(min_year, st.session_state.ts_year_start), min(max_year, st.session_state.ts_year_end)),
    key="ts_year_slider"
)
st.session_state.ts_year_start = year_range[0]
st.session_state.ts_year_end = year_range[1]

# 7. Chart options
st.sidebar.markdown("---")
st.sidebar.subheader("Chart Options")
normalize = st.sidebar.checkbox("Normalize (Index to 100)", value=st.session_state.ts_normalize, key="ts_normalize_cb")
st.session_state.ts_normalize = normalize

show_markers = st.sidebar.checkbox("Show Markers", value=st.session_state.ts_markers, key="ts_markers_cb")
st.session_state.ts_markers = show_markers

use_dual_axis = st.sidebar.checkbox("Use Dual Y-Axis", value=st.session_state.ts_dual_axis or len(selected_indicators) > 1, key="ts_dual_cb")
st.session_state.ts_dual_axis = use_dual_axis

# Fetch data
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

def get_units_label(ind_code):
    if normalize:
        return "Index (Base=100)"
    units = indicator_units.get(ind_code, '')
    return units if units else "Value"

# Create chart
if len(selected_indicators) == 1:
    ind_code = selected_indicators[0]
    ind_name = indicator_options.get(ind_code, ind_code)
    units_label = get_units_label(ind_code)
    
    st.markdown(f"### {ind_name}")
    
    df = all_data[0].copy()
    
    if normalize:
        for country in df['country_iso3'].unique():
            mask = df['country_iso3'] == country
            sorted_df = df.loc[mask].sort_values('year')
            if len(sorted_df) > 0:
                first_val = sorted_df['value'].iloc[0]
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
        yaxis_title=units_label,
        hovermode="x unified",
        height=500
    )
    st.plotly_chart(fig, use_container_width=True)

else:
    st.markdown("### Multi-Indicator Comparison")
    
    if len(selected_countries) == 1:
        country_name = country_options.get(selected_countries[0], selected_countries[0])
        st.markdown(f"**Country:** {country_name}")
        
        if use_dual_axis and len(selected_indicators) >= 2:
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            
            left_indicators = selected_indicators[:len(selected_indicators)//2 + len(selected_indicators)%2]
            right_indicators = selected_indicators[len(selected_indicators)//2 + len(selected_indicators)%2:]
            
            left_units = get_units_label(left_indicators[0]) if left_indicators else "Value"
            right_units = get_units_label(right_indicators[0]) if right_indicators else "Value"
            
            for i, ind_code in enumerate(selected_indicators):
                df = [d for d in all_data if d['indicator_code'].iloc[0] == ind_code]
                if df:
                    df = df[0].copy()
                    if normalize:
                        first_val = df.sort_values('year')['value'].iloc[0]
                        if first_val and first_val != 0:
                            df['value'] = (df['value'] / first_val) * 100
                    
                    df = df.sort_values('year')
                    secondary = ind_code in right_indicators
                    
                    # Shorter name for legend
                    short_name = indicator_options.get(ind_code, ind_code).split(' (')[0][:40]
                    
                    fig.add_trace(
                        go.Scatter(
                            x=df['year'],
                            y=df['value'],
                            mode='lines+markers' if show_markers else 'lines',
                            name=short_name,
                            line=dict(color=colors[i % len(colors)])
                        ),
                        secondary_y=secondary
                    )
            
            fig.update_layout(xaxis_title="Year", hovermode="x unified", height=500)
            fig.update_yaxes(title_text=left_units, secondary_y=False)
            fig.update_yaxes(title_text=right_units, secondary_y=True)
            
        else:
            fig = go.Figure()
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']
            
            for i, ind_code in enumerate(selected_indicators):
                df = [d for d in all_data if d['indicator_code'].iloc[0] == ind_code]
                if df:
                    df = df[0].copy()
                    if normalize:
                        first_val = df.sort_values('year')['value'].iloc[0]
                        if first_val and first_val != 0:
                            df['value'] = (df['value'] / first_val) * 100
                    
                    df = df.sort_values('year')
                    short_name = indicator_options.get(ind_code, ind_code).split(' (')[0][:40]
                    fig.add_trace(go.Scatter(
                        x=df['year'],
                        y=df['value'],
                        mode='lines+markers' if show_markers else 'lines',
                        name=short_name,
                        line=dict(color=colors[i % len(colors)])
                    ))
            
            fig.update_layout(
                xaxis_title="Year",
                yaxis_title="Value" if normalize else "Mixed Units",
                hovermode="x unified",
                height=500
            )
        
        st.plotly_chart(fig, use_container_width=True)
    
    else:
        ind_code = selected_indicators[0]
        ind_name = indicator_options.get(ind_code, ind_code)
        units_label = get_units_label(ind_code)
        
        st.markdown(f"**Indicator:** {ind_name}")
        
        df = all_data[0].copy()
        
        if normalize:
            for country in df['country_iso3'].unique():
                mask = df['country_iso3'] == country
                sorted_df = df.loc[mask].sort_values('year')
                if len(sorted_df) > 0:
                    first_val = sorted_df['value'].iloc[0]
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
            yaxis_title=units_label,
            hovermode="x unified",
            height=500
        )
        st.plotly_chart(fig, use_container_width=True)
        
        if len(selected_indicators) > 1:
            st.info("💡 Select a single country to compare multiple indicators")

# Data table
st.markdown("---")
st.markdown("### 📋 Data")
display_df = combined_df[['year', 'country_name', 'indicator_name', 'value', 'source']].copy()
display_df = display_df.sort_values(['indicator_name', 'country_name', 'year'])
st.dataframe(display_df, use_container_width=True, hide_index=True)

csv = display_df.to_csv(index=False)
st.download_button("📥 Download CSV", data=csv, file_name="timeseries_data.csv", mime="text/csv")
