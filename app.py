"""
NASA Events 2025 - Streamlit Data Dashboard

This app displays NASA EONET natural events data with visualizations:
- Daily event count (time series)
- Total event count (big number)
- Events by category (radial chart)
- Geospatial event distribution (world map)
"""

import streamlit as st
import pandas as pd
import altair as alt
from pathlib import Path
import os
from datetime import datetime, timedelta
from data_pipeline import extract_events, transform_events

# ============================================================================
# Page Configuration
# ============================================================================
st.set_page_config(
    page_title="NASA Events 2025",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================================
# Helper Functions
# ============================================================================

@st.cache_data
def load_events_data():
    """Load events data by calling the EONET API for the last 5 years (end = yesterday).

    This replaces reading local CSVs and uses the pipeline extraction + transform functions.
    Returns a DataFrame with the transformed events.
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
        df = transform_events(api_data)
    except Exception as e:
        st.error(f"Failed to fetch/transform API data: {e}")
        st.stop()

    if df.empty:
        st.warning("No events returned from the API for the requested range.")

    return df


@st.cache_data
def load_category_counts():
    """Load category count data."""
    data_path = Path("data/processed/events_2025_category_counts.csv")
    
    if not data_path.exists():
        return None
    
    df_cat = pd.read_csv(data_path)
    # Normalize column names if needed
    if 'category_titles' in df_cat.columns and 'category_title' not in df_cat.columns:
        df_cat = df_cat.rename(columns={'category_titles': 'category_title'})
    if 'count' in df_cat.columns and 'event_count' not in df_cat.columns:
        df_cat = df_cat.rename(columns={'count': 'event_count'})
    
    # Coerce to numeric
    df_cat['event_count'] = pd.to_numeric(df_cat['event_count'], errors='coerce').fillna(0)
    
    return df_cat


def create_daily_chart(df):
    """Create daily event count line chart."""
    df_with_dates = df[df['occurrence_date'].notna()].copy()
    if df_with_dates.empty:
        return None
    
    df_with_dates['date'] = df_with_dates['occurrence_date'].dt.normalize()
    daily_counts = df_with_dates.groupby('date').size().reset_index(name='event_count')
    
    chart = alt.Chart(daily_counts).mark_line(point=True).encode(
        x=alt.X('date:T', title='Date', axis=alt.Axis(format='%Y-%m-%d')),
        y=alt.Y('event_count:Q', title='Number of Events'),
        tooltip=['date:T', 'event_count:Q']
    ).properties(
        title='Daily Event Count',
        width=400,
        height=300
    )
    
    return chart


def create_big_number_chart(df):
    """Create big number chart showing total event count."""
    total_events = len(df)
    
    # Simple text display as a number
    source = pd.DataFrame({
        "metric": ["Total Events"],
        "value": [total_events]
    })
    
    chart = alt.Chart(source).mark_text(
        size=60,
        fontWeight='bold',
        color='#1f77b4'
    ).encode(
        text=alt.Text('value:Q', format=',.0f')
    ).properties(
        title='Total Number of Events (2025)',
        width=400,
        height=150
    )
    
    return chart


def create_radial_chart(df_categories):
    """Create radial (donut) chart showing events by category."""
    if df_categories is None or df_categories.empty:
        return None
    
    # Ensure columns exist
    if 'category_title' not in df_categories.columns:
        df_categories = df_categories.rename(columns={df_categories.columns[0]: 'category_title'})
    if 'event_count' not in df_categories.columns:
        df_categories = df_categories.rename(columns={df_categories.columns[1]: 'event_count'})
    
    chart = alt.Chart(df_categories).mark_arc(
        innerRadius=60,
        outerRadius=100
    ).encode(
        theta=alt.Theta('event_count:Q', stack=True),
        color=alt.Color('category_title:N', title='Category'),
        tooltip=[
            alt.Tooltip('category_title:N', title='Category'),
            alt.Tooltip('event_count:Q', title='Count', format=',.0f')
        ]
    ).properties(
        title='Events by Category',
        width=400,
        height=300
    )

    return chart


def create_geospatial_map(df):
    """Create the geospatial Altair map from a dataframe.

    Expects columns: latitude, longitude, event_title, category_titles, occurrence_date
    Returns an Altair chart or None when no valid coords are present.
    """
    try:
        from vega_datasets import data
    except Exception:
        # vega_datasets may not be installed in the environment; return None gracefully
        return None

    # Filter out rows with missing coordinates
    df_geo = df[(df.get('latitude').notna()) & (df.get('longitude').notna())].copy()

    if df_geo.empty:
        # No geographic data
        return None
    
    # Make sure occurrence_date is datetime for proper tooltip formatting
    if 'occurrence_date' in df_geo.columns:
        df_geo['occurrence_date'] = pd.to_datetime(df_geo['occurrence_date'], errors='coerce')

    # Normalize possible column name differences
    if 'category_title' in df_geo.columns and 'category_titles' not in df_geo.columns:
        df_geo = df_geo.rename(columns={'category_title': 'category_titles'})

    # Create the geospatial map
    countries = alt.topo_feature(data.world_110m.url, 'countries')

    world_bg = (
        alt.Chart(countries)
        .mark_geoshape(fill='#f5f5f5', stroke='#bdbdbd', strokeWidth=0.5)
    )

    map_points = alt.Chart(df_geo).mark_circle(
        size=50,
        opacity=0.6
    ).encode(
        longitude='longitude:Q',
        latitude='latitude:Q',
        color=alt.Color('category_titles:N', title='Event Category'),
        tooltip=[
            alt.Tooltip('event_title:N', title='Event'),
            alt.Tooltip('category_titles:N', title='Category'),
            alt.Tooltip('occurrence_date:T', title='Date'),
            alt.Tooltip('latitude:Q', title='Latitude', format='.2f'),
            alt.Tooltip('longitude:Q', title='Longitude', format='.2f')
        ]
    )

    map_chart = (
        (world_bg + map_points)
        .project(type='naturalEarth1')
        .properties(title='Geospatial Event Map', width=1000, height=600)
    )

    return map_chart


def main():
    # Header
    st.title("🌍 NASA Events 2025")
    st.subheader("Global")
    st.divider()

    # Load data
    with st.spinner("Loading data..."):
        df = load_events_data()

    # Ensure year column exists
    if 'year' not in df.columns:
        if 'occurrence_date' in df.columns:
            df['year'] = df['occurrence_date'].dt.year
        else:
            df['year'] = None

    # Build year selection options
    available_years = sorted([int(y) for y in df['year'].dropna().unique()], reverse=True) if not df.empty else []
    year_options = ["All years"] + [str(y) for y in available_years]
    current_year_str = str(datetime.now().year)
    # Default to current year if available, else first numeric year, else All years
    if current_year_str in year_options:
        default_index = year_options.index(current_year_str)
    elif len(year_options) > 1:
        default_index = 1
    else:
        default_index = 0

    selected_year = st.selectbox("Select year", year_options, index=default_index)

    # Filter dataframe by selected year
    if selected_year == "All years":
        df_filtered = df.copy()
    else:
        try:
            sel_year_int = int(selected_year)
            df_filtered = df[df['year'] == sel_year_int].copy()
        except Exception:
            df_filtered = df.copy()

    # Derive category counts from filtered data (prefer live API data)
    df_categories = None
    if not df_filtered.empty and 'category_titles' in df_filtered.columns:
        df_categories = (
            df_filtered[['category_titles', 'event_id']]
            .drop_duplicates()
            .groupby('category_titles')
            .agg(event_count=('event_id', 'nunique'))
            .reset_index()
            .rename(columns={'category_titles': 'category_title'})
        )
    else:
        # Fallback to loading precomputed category counts
        df_cat_loaded = load_category_counts()
        if df_cat_loaded is not None:
            # If the precomputed file has a year column, filter it
            if 'year' in df_cat_loaded.columns and selected_year != "All years":
                try:
                    df_categories = df_cat_loaded[df_cat_loaded['year'] == int(selected_year)].copy()
                except Exception:
                    df_categories = df_cat_loaded
            else:
                df_categories = df_cat_loaded

    # Create charts using the filtered dataset
    with st.spinner("Generating charts..."):
        daily_chart = create_daily_chart(df_filtered)
        big_number_chart = create_big_number_chart(df_filtered)
        radial_chart = create_radial_chart(df_categories)
        map_chart = create_geospatial_map(df_filtered)

    # Layout: 2 columns (25% left, 75% right)
    col_left, col_right = st.columns([1, 3])

    # ========================================================================
    # Left Column (25%) - 3 Equal Sections
    # ========================================================================
    with col_left:
        st.subheader("Summary")

        # Top section: Daily chart
        st.markdown("#### Daily Events")
        if daily_chart:
            st.altair_chart(daily_chart, use_container_width=True)
        else:
            st.warning("No daily event data available for the selected year.")

        st.divider()

        # Middle section: Big number
        st.markdown("#### Total Events")
        if big_number_chart:
            st.altair_chart(big_number_chart, use_container_width=True)
        else:
            st.warning("No event count data available for the selected year.")

        st.divider()

        # Bottom section: Radial chart
        st.markdown("#### Events by Category")
        if radial_chart:
            st.altair_chart(radial_chart, use_container_width=True)
        else:
            st.warning("No category data available for the selected year.")

    # ========================================================================
    # Right Column (75%) - Geospatial Map
    # ========================================================================
    with col_right:
        st.subheader("Geospatial View")

        if map_chart:
            st.altair_chart(map_chart, use_container_width=True)
        else:
            st.warning("No geographic data available for the selected year.")

    # Footer
    st.divider()
    st.markdown(
        """
        **Data Source:** NASA EONET (Earth Observatory Natural Event Tracker)  
        **Last Updated:** 2025-11-13  
        **Dashboard:** Built with [Streamlit](https://streamlit.io) and [Altair](https://altair-viz.github.io)
        """
    )
    


if __name__ == "__main__":
    main()
