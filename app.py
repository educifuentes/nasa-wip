"""
NASA Events 2025 - Streamlit Data Dashboard

This app displays NASA EONET natural events data with visualizations:
- Total event count (big number)
- Geospatial event distribution (world map)
"""

import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
from datetime import datetime, timedelta
from src.data_pipeline import extract_events, transform_events
from src.charts import (
    create_daily_chart,
    create_big_number_chart,
    create_radial_chart,
    create_geospatial_map,
)

st.set_page_config(
    page_title="NASA Events 2025",
    page_icon=":earth_africa:",
    layout="wide",
)

""  # Add some space.

# Main header - shown from the start
st.markdown("# :material/earthquake: NASA Natural Events")

# Placeholder for subheader that will be updated after data loads
subheader_placeholder = st.empty()

# Show initial subheader with "..."
subheader_placeholder.markdown(
    """
    Explore natural events tracked by NASA's Earth Observatory Natural Event Tracker (EONET).  
    Last update: ...
    """
)

cols = st.columns([1, 3])
# Will declare right cell later to avoid showing it when no data.


@st.cache_data
def load_events_data():
    """Load events data by calling the EONET API for the last 5 years (end = yesterday).

    This replaces reading local CSVs and uses the pipeline extraction + transform functions.
    Returns both events_df and occurrences_df.
    """
    # Compute date range: last 5 years up to yesterday
    end_dt = datetime.now() - timedelta(days=1)
    # Approximate 5 years as 5*365 days to avoid adding a heavy dependency
    start_dt = end_dt - timedelta(days=5 * 365)
    start_date = start_dt.strftime("%Y-%m-%d")
    end_date = end_dt.strftime("%Y-%m-%d")

    # Extract from API and transform using existing pipeline functions
    try:
        api_data = extract_events(start_date, end_date)
        events_df, occurrences_df = transform_events(api_data)
    except Exception as e:
        st.error(f"Failed to fetch/transform API data: {e}")
        st.stop()

    if occurrences_df.empty:
        st.warning("No events returned from the API for the requested range.")

    # Return both dataframes and end_dt
    return events_df, occurrences_df, end_dt


# Load data
with st.spinner("Loading data..."):
    events_df, occurrences_df, end_dt = load_events_data()

# Update the subheader with the actual date
subheader_placeholder.markdown(
    f"""
    Explore natural events tracked by NASA's Earth Observatory Natural Event Tracker (EONET).  
    Last update: {end_dt.strftime("%Y-%m-%d")}
    """
)

# Ensure year column exists in occurrences_df
if 'year' not in occurrences_df.columns:
    if 'occurrence_date' in occurrences_df.columns:
        occurrences_df['year'] = occurrences_df['occurrence_date'].dt.year
    elif 'date' in occurrences_df.columns:
        occurrences_df['year'] = pd.to_datetime(occurrences_df['date'], errors='coerce').dt.year
    else:
        occurrences_df['year'] = None

# Ensure year column exists in events_df
if 'year' not in events_df.columns:
    if 'date' in events_df.columns:
        events_df['year'] = pd.to_datetime(events_df['date'], errors='coerce').dt.year
    else:
        events_df['year'] = None

# Build year selection options from occurrences_df (more comprehensive)
available_years = sorted([int(y) for y in occurrences_df['year'].dropna().unique()], reverse=True) if not occurrences_df.empty else []
year_options = ["All years"] + [str(y) for y in available_years]
current_year_str = str(datetime.now().year)
# Default to current year if available, else first numeric year, else All years
if current_year_str in year_options:
    default_index = year_options.index(current_year_str)
elif len(year_options) > 1:
    default_index = 1
else:
    default_index = 0

top_left_cell = cols[0].container(
    border=True, height="stretch", vertical_alignment="center"
)

with top_left_cell:
    # Year selector
    selected_year = st.selectbox("Select year", year_options, index=default_index)

    # Region (continent) selection - default Global (no filter)
    available_continents = []
    if not occurrences_df.empty and 'continent' in occurrences_df.columns:
        available_continents = sorted([c for c in occurrences_df['continent'].dropna().unique()])
    region_options = ["Global"] + available_continents
    selected_region = st.selectbox("Select region (continent)", region_options, index=0)

# Filter occurrences_df by selected year
if selected_year == "All years":
    occurrences_df_filtered = occurrences_df.copy()
else:
    try:
        sel_year_int = int(selected_year)
        occurrences_df_filtered = occurrences_df[occurrences_df['year'] == sel_year_int].copy()
    except Exception:
        occurrences_df_filtered = occurrences_df.copy()

# Filter events_df by selected year
# If events_df has a year column, use it; otherwise filter by matching event_ids from filtered occurrences
if selected_year == "All years":
    events_df_filtered = events_df.copy()
else:
    try:
        sel_year_int = int(selected_year)
        # First try to filter by year column if it exists and has valid values
        if 'year' in events_df.columns and events_df['year'].notna().any():
            events_df_filtered = events_df[events_df['year'] == sel_year_int].copy()
        else:
            # Filter events_df to only include events that have occurrences in the selected year
            if not occurrences_df_filtered.empty and 'event_id' in occurrences_df_filtered.columns:
                event_ids_in_year = occurrences_df_filtered['event_id'].unique()
                events_df_filtered = events_df[events_df['event_id'].isin(event_ids_in_year)].copy()
            else:
                events_df_filtered = events_df.copy()
    except Exception:
        events_df_filtered = events_df.copy()

# Apply region filter to occurrences_df if not Global
if selected_region != "Global":
    occurrences_df_filtered = occurrences_df_filtered[occurrences_df_filtered['continent'] == selected_region].copy()

if occurrences_df_filtered.empty and events_df_filtered.empty:
    top_left_cell.info("No data available for the selected filters.", icon=":material/info:")
    st.stop()

# Derive category counts from filtered events_df for radial chart
df_categories = None
if not events_df_filtered.empty and 'categories' in events_df_filtered.columns:
    # events_df has 'categories' as a list, need to explode and count
    categories_expanded = events_df_filtered.explode('categories')
    df_categories = (
        categories_expanded.groupby('categories')
        .agg(event_count=('event_id', 'nunique'))
        .reset_index()
        .rename(columns={'categories': 'category_title'})
    )
elif not events_df_filtered.empty and 'category_titles' in events_df_filtered.columns:
    # If category_titles exists as a string column
    df_categories = (
        events_df_filtered.groupby('category_titles')
        .agg(event_count=('event_id', 'nunique'))
        .reset_index()
        .rename(columns={'category_titles': 'category_title'})
    )

# Create charts using the filtered datasets
with st.spinner("Generating charts..."):
    daily_chart = create_daily_chart(occurrences_df_filtered)
    big_number_chart = create_big_number_chart(occurrences_df_filtered)
    radial_chart = create_radial_chart(df_categories)
    map_chart = create_geospatial_map(occurrences_df_filtered)

right_cell = cols[1].container(
    border=True, height="stretch", vertical_alignment="center"
)

bottom_left_cell = cols[0].container(
    border=True, height="stretch", vertical_alignment="center"
)

with bottom_left_cell:
    if big_number_chart:
        st.altair_chart(big_number_chart, use_container_width=True)
    else:
        st.warning("No event count data available for the selected filters.")

# Plot geospatial map
with right_cell:
    if map_chart:
        st.altair_chart(map_chart, use_container_width=True)
    else:
        st.warning("No geographic data available for the selected filters.")

""  # Add some space.

cols2 = st.columns([2, 1])

left_chart_cell = cols2[0].container(
    border=True, height="stretch", vertical_alignment="center"
)

right_chart_cell = cols2[1].container(
    border=True, height="stretch", vertical_alignment="center"
)

with left_chart_cell:
    if daily_chart:
        st.altair_chart(daily_chart, use_container_width=True)
    else:
        st.warning("No daily event data available for the selected filters.")

with right_chart_cell:
    if radial_chart:
        st.altair_chart(radial_chart, use_container_width=True)
    else:
        st.warning("No category data available for the selected filters.")

""
""
