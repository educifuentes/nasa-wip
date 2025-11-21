import pandas as pd
import altair as alt
from vega_datasets import data


def create_daily_chart(df):
    """Create daily occurrence count line chart."""

    agg_df = df.groupby(df['date']).agg(count=('date', 'size')).reset_index()
    agg_df['date'] = pd.to_datetime(agg_df['date'])

    emerald = "#10b981"  # Tailwind emerald-500

    chart = (
        alt.Chart(agg_df)
        .mark_line(point=alt.OverlayMarkDef(color=emerald), color=emerald)
        .encode(
            x=alt.X(
                'date:T', title=None,
                axis=alt.Axis(
                    format='%Y-%m-%d',
                    labelAngle=-45,
                    labelFontSize=9
                )
            ),
            y=alt.Y('count:Q', title=None),
            tooltip=['date:T', 'count:Q']
        )
        .properties(
            width=600,
            height=300
        )
    )

    return chart


def create_big_number_chart(df):
    """Create big number chart showing the number of occurrences that
    occurred on the most recent date with at least one occurrence.

    The function expects an occurrences_df with a `date` column (coerced to datetimes)
    representing the occurrence date for each row. It finds the most recent
    date with at least one occurrence and counts the number of occurrences (rows) on that date.
    Returns None when no valid date data is found.
    """
    if df is None or df.empty:
        return None

    # Ensure date column exists and is datetime
    if 'date' not in df.columns:
        return None

    dates = pd.to_datetime(df['date'], errors='coerce')
    
    # Normalize to date (remove time)
    normalized = dates.dt.normalize()
    
    # Remove rows with invalid dates
    valid_mask = normalized.notna()
    if not valid_mask.any():
        return None

    # Find the most recent date with at least one occurrence
    most_recent_date = normalized[valid_mask].max()
    
    if pd.isna(most_recent_date):
        return None

    # Count occurrences on the most recent date
    mask = normalized == most_recent_date
    
    if not mask.any():
        return None

    # Count occurrences (rows) on that date
    value = int(mask.sum())

    source = pd.DataFrame({
        "metric": ["Occurrences"],
        "value": [value]
    })

    chart = alt.Chart(source).mark_text(
        size=60,
        fontWeight='bold',
        color='#1f77b4'
    ).encode(
        text=alt.Text('value:Q', format=',.0f')
    ).properties(
        title=f'Occurrences on {most_recent_date.date()}',
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


def create_geospatial_map(df, region="world"):
    """Create the geospatial Altair map from a dataframe.

    Expects columns: latitude, longitude, event_title, category_titles, occurrence_date
    Returns an Altair chart or None when no valid coords are present.
    """
    # Filter out rows with missing coordinates
    df_geo = df[(df.get('latitude').notna()) & (df.get('longitude').notna())].copy()

    if df_geo.empty:
        # No geographic data
        return None

    # Make sure occurrence_date is datetime for proper tooltip formatting
    if 'date' in df_geo.columns:
        df_geo['date'] = pd.to_datetime(df_geo['date'], errors='coerce')

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
            alt.Tooltip('date:T', title='Date'),
            alt.Tooltip('latitude:Q', title='Latitude', format='.2f'),
            alt.Tooltip('longitude:Q', title='Longitude', format='.2f')
        ]
    )

    map_chart = (
        (world_bg + map_points)
        .project(type='naturalEarth1')
        .properties(title="", width=1000, height=600)
    )

    return map_chart
