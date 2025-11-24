"""
NASA EONET Data Pipeline
Extracts, transforms, and loads natural events data from NASA EONET API V3
"""

import requests
import pandas as pd
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import reverse_geocoder as rg
import pycountry
# No longer using pycountry_convert; continent is inferred from lat/lon heuristics
pc = None


def extract_events(start_date: str = "2020-01-01", end_date: str = None) -> dict:
    """
    Extract natural events from NASA EONET API V3 within a date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format (default: 2020-01-01)
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        Dictionary containing API response
    """
    base_url = "https://eonet.gsfc.nasa.gov/api/v3/events"
    params = {
        "start": start_date,
        "end": end_date
    }

    start_date = start_date or "2024-01-01"
    end_date = end_date or datetime.now().strftime("%Y-%m-%d")  
    
    print(f"Extracting events from {start_date} to {end_date}...")
    
    try:
        response = requests.get(base_url, params=params)
        response.raise_for_status()
        data = response.json()
        print(f"Successfully extracted {len(data.get('events', []))} events")
        return data
    except requests.exceptions.RequestException as e:
        print(f"Error extracting data: {e}")
        raise


def transform_events(api_data: dict) -> tuple:
    """
    Transform and clean the extracted events data.
    
    Args:
        api_data: Raw API response dictionary
    
    Returns:
        DataFrame with transformed events data
    """
    events = api_data.get('events', [])
    
    if not events:
        print("No events found in the API response")
        return pd.DataFrame()
    
    # Flatten the nested structure
    transformed_data = []
    # Build events metadata list separately
    events_meta = []
    
    for event in events:
        event_id = event.get('id')
        event_title = event.get('title', '')
        event_description = event.get('description', '')
        event_link = event.get('link', '')
        
        # Extract categories
        categories = event.get('categories', [])
        category_ids = [cat.get('id') for cat in categories]
        category_titles = [cat.get('title', '') for cat in categories]
        
        # Extract geometry/occurrences
        geometry = event.get('geometry', [])
        
        if not geometry:
            # If no geometry, create one record with event info
            transformed_data.append({
                'event_id': event_id,
                'event_title': event_title,
                'event_description': event_description,
                'event_link': event_link,
                'category_ids': ','.join(map(str, category_ids)),
                'category_titles': ','.join(category_titles),
                'date': None,
                'occurrence_type': None,
                'longitude': None,
                'latitude': None
            })
        else:
            # Create one record per occurrence/geometry
            for geom in geometry:
                coordinates = geom.get('coordinates', [])
                longitude = coordinates[0] if len(coordinates) > 0 else None
                latitude = coordinates[1] if len(coordinates) > 1 else None
                date_str = geom.get('date', '')
                
                transformed_data.append({
                    'event_id': event_id,
                    'event_title': event_title,
                    'event_description': event_description,
                    'event_link': event_link,
                    'category_ids': ','.join(map(str, category_ids)),
                    'category_titles': ','.join(category_titles),
                    'date': date_str,
                    'occurrence_type': geom.get('type', ''),
                    'longitude': longitude,
                    'latitude': latitude
                })
    
    occurrences_df = pd.DataFrame(transformed_data)

    # Build events_df with static metadata (one row per event)
    for event in events:
        events_meta.append({
            'event_id': event.get('id'),
            'title': event.get('title'),
            'description': event.get('description', ''),
            'link': event.get('link', ''),
            'categories': [c['title'] for c in event.get('categories', [])],
            'sources': [s['id'] for s in event.get('sources', [])],
            'closed': event.get('closed', None),
            'status': event.get('status', None)
        })

    events_df = pd.DataFrame(events_meta)
    
    # Data cleaning
    if not occurrences_df.empty:
        # Convert date column to datetime
        occurrences_df['date'] = pd.to_datetime(occurrences_df['date'], errors='coerce')

        # Extract year for filtering
        occurrences_df['year'] = occurrences_df['date'].dt.year
        occurrences_df['month'] = occurrences_df['date'].dt.month
        occurrences_df['day'] = occurrences_df['date'].dt.day
        
        # Remove rows with invalid coordinates (if needed)
        # Keep them for now as they might have other useful data
        
        print(f"Transformed {len(occurrences_df)} occurrences from {len(events)} events")
    
    # Return both events (metadata) and occurrences (time-series) DataFrames
    return events_df, occurrences_df


def save_data(df: pd.DataFrame, output_path: str):
    """
    Save the preprocessed dataset to CSV.
    
    Args:
        df: DataFrame to save
        output_path: Path to save the CSV file
    """
    # Ensure directory exists
    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    
    df.to_csv(output_path, index=False)
    print(f"Data saved to {output_path}")


def enrich_with_reverse_geocoding(df: pd.DataFrame) -> pd.DataFrame:
    """Add `region` and `country` columns to dataframe using reverse_geocoder.

    This function will attempt to reverse-geocode unique (lat, lon) pairs to
    reduce repeated lookups. If reverse_geocoder is not available or no valid
    coordinates exist, the function will add the columns with None values.
    """
    if df is None or df.empty:
        return df

    # Prepare new columns
    df['region'] = None
    df['country'] = None
    df['continent'] = None

    # Ensure latitude/longitude columns exist
    if 'latitude' not in df.columns or 'longitude' not in df.columns:
        print("Latitude/longitude columns not found; skipping reverse geocoding")
        return df

    # Filter rows with valid coordinates
    coord_mask = df['latitude'].notna() & df['longitude'].notna()
    if not coord_mask.any():
        print("No valid coordinates found; skipping reverse geocoding")
        return df

    # Build list of unique rounded coordinate tuples to reduce API calls
    coords_df = (
        df.loc[coord_mask, ['latitude', 'longitude']]
        .drop_duplicates()
    )

    # Round coordinates to 4 decimal places to group nearby points
    coords = [ (round(float(r.latitude), 4), round(float(r.longitude), 4)) for r in coords_df.itertuples() ]

    try:
        results = rg.search(coords)  # returns list of dicts with keys like 'admin1', 'cc'
    except Exception as e:
        print(f"Reverse geocoding failed: {e}. Skipping geocoding step.")
        return df

    # Map rounded coord -> geocode result
    coord_to_result = { coords[i]: results[i] for i in range(len(coords)) }

    # Helper to lookup country name from country code
    def country_name_from_cc(cc):
        try:
            country = pycountry.countries.get(alpha_2=cc.upper())
            return country.name if country is not None else cc
        except Exception:
            return cc

    def continent_from_latlon(lat, lon):
        """Heuristic continent lookup based only on latitude and longitude.

        This uses simple bounding-box rules and is intentionally lightweight.
        It is not perfect (border cases and islands may be misclassified),
        but works well for bulk assignments without external dependencies.
        """
        try:
            if pd.isna(lat) or pd.isna(lon):
                return None
            lat = float(lat)
            lon = float(lon)
        except Exception:
            return None

        # Normalize longitude to [-180, 180]
        if lon > 180:
            lon = ((lon + 180) % 360) - 180
        if lon < -180:
            lon = ((lon + 180) % 360) - 180

        # Antarctica
        if lat <= -60:
            return 'Antarctica'

        # Oceania / Australia / Pacific islands
        if (lon >= 110 and lon <= 180) and (lat >= -50 and lat <= 10):
            return 'Oceania'
        # Also include some Pacific longitudes that wrap negative
        if (lon >= -180 and lon <= -140) and (lat >= -50 and lat <= 10):
            return 'Oceania'

        # South America
        if lon >= -82 and lon <= -34 and lat >= -56 and lat <= 13:
            return 'South America'

        # North America (including Central America/Caribbean if lat > -15)
        if lon >= -170 and lon <= -50 and lat >= -15 and lat <= 83:
            return 'North America'

        # Africa
        if lon >= -20 and lon <= 60 and lat >= -35 and lat <= 38:
            return 'Africa'

        # Europe
        if lon >= -25 and lon <= 45 and lat >= 36 and lat <= 72:
            return 'Europe'

        # Asia (fallback for large eastern longitudes)
        if lon > 45 or lon < -25:
            return 'Asia'

        # As a final fallback
        return None

    # Apply mapping back to dataframe rows (using rounded keys)
    def lookup_region_country(lat, lon):
        if pd.isna(lat) or pd.isna(lon):
            return (None, None)
        key = (round(float(lat), 4), round(float(lon), 4))
        res = coord_to_result.get(key)
        if not res:
            return (None, None)
        region = res.get('admin1') or res.get('admin2') or None
        cc = res.get('cc')
        country = country_name_from_cc(cc) if cc else None
        return (region, country)

    # Vectorized application
    regions = []
    countries = []
    continents = []
    for lat, lon in zip(df['latitude'], df['longitude']):
        r, c = lookup_region_country(lat, lon)
        regions.append(r)
        countries.append(c)
        # Infer continent solely from lat/lon
        continents.append(continent_from_latlon(lat, lon))

    df['region'] = regions
    df['country'] = countries
    df['continent'] = continents

    print(f"Reverse geocoding added 'region' and 'country' for {coord_mask.sum()} rows")
    return df


def add_first_occurrence_to_events(events_df: pd.DataFrame, occurrences_df: pd.DataFrame) -> pd.DataFrame:
    """Add first-occurrence columns to `events_df`.

    For each `event_id` in `events_df`, find the earliest occurrence in
    `occurrences_df` (based on `occurrence_date`) and add the following
    columns to the events table:

    - `category_titles` (from the occurrence row)
    - `date` (the earliest `occurrence_date`)
    - `longitude`, `latitude`
    - `continent`, `country`

    If an event has no occurrences, the new columns will be NaN/None.
    """
    if events_df is None or events_df.empty:
        return events_df

    if occurrences_df is None or occurrences_df.empty:
        # Ensure the columns exist on events_df for downstream consumers
        for col in ['category_titles', 'date', 'longitude', 'latitude', 'continent', 'country']:
            if col not in events_df.columns:
                events_df[col] = None
        return events_df

    occ = occurrences_df.copy()

    # Ensure date is datetime for proper ordering
    if 'date' in occ.columns:
        occ['date'] = pd.to_datetime(occ['date'], errors='coerce')
    else:
        occ['date'] = pd.NaT

    # Sort and pick the first (earliest) occurrence per event_id
    occ_sorted = occ.sort_values(['event_id', 'date'], ascending=[True, True])

    # Keep the first non-null date per event. Using groupby+first preserves other columns.
    first_occ = occ_sorted.groupby('event_id', as_index=False).first()

    # Prepare columns to merge.
    cols_map = {}
    desired_cols = ['category_titles', 'date', 'longitude', 'latitude', 'continent', 'country']
    available = [c for c in desired_cols if c in first_occ.columns]

    merge_cols = ['event_id'] + available
    first_subset = first_occ[merge_cols].copy()

    # Merge into events_df (left join to keep all events)
    merged = events_df.merge(first_subset, on='event_id', how='left')

    # Ensure all requested columns exist on the resulting DataFrame
    for col in ['category_titles', 'date', 'longitude', 'latitude', 'continent', 'country']:
        if col not in merged.columns:
            merged[col] = None

    print(f"Added first-occurrence columns for {merged['event_id'].notna().sum()} events (where available)")
    return merged


def clean_occurrences(occurrences_df: pd.DataFrame, start_date: str) -> pd.DataFrame:
    """Filter `occurrences_df` to remove occurrences earlier than `start_date`.

    Behavior:
    - If `date` exists, rows with `date` < start_date are dropped.
    - Else, if a `year` column exists, rows with `year` < start_year are dropped.
    - If neither column exists, the DataFrame is returned unchanged.

    Args:
        occurrences_df: DataFrame containing occurrence rows with at least `event_id`.
        start_date: Start date string in `YYYY-MM-DD` format.

    Returns:
        Filtered occurrences_df with only rows on or after the start_date (or start_year).
    """
    if occurrences_df is None:
        return occurrences_df

    if occurrences_df.empty:
        return occurrences_df

    # Parse start_date to datetime and year
    try:
        start_dt = pd.to_datetime(start_date)
        start_year = int(start_dt.year)
    except Exception:
        # If parsing fails, do not filter
        print(f"Warning: could not parse start_date='{start_date}'; skipping cleaning by date")
        return occurrences_df

    occ = occurrences_df.copy()

    if 'date' in occ.columns:
        occ['date'] = pd.to_datetime(occ['date'], errors='coerce')
        before_count = len(occ)
        occ = occ[occ['date'].notna() & (occ['date'] >= start_dt) | occ['date'].isna()]
        after_count = len(occ)
        print(f"clean_occurrences: removed {before_count - after_count} rows older than {start_date} based on date")
        return occ

    if 'year' in occ.columns:
        before_count = len(occ)
        # Keep rows where year >= start_year
        occ = occ[occ['year'].fillna(start_year) >= start_year]
        after_count = len(occ)
        print(f"clean_occurrences: removed {before_count - after_count} rows with year < {start_year}")
        return occ

    # No date/year columns available; return as-is
    print("clean_occurrences: no 'date' or 'year' column found; no filtering applied")
    return occ


def clean_and_prepare_occurrences(occurrences_df: pd.DataFrame, start_date: str, occ_output_path: str) -> pd.DataFrame:
    """Clean and prepare occurrences dataframe for saving.
    
    This function:
    1. Cleans occurrences to remove rows earlier than start_date
    2. Merges with existing data if file exists
    3. Drops duplicates based on event_id and date
    4. Filters to years 2024 and 2025
    5. Drops duplicate rows (general deduplication)
    
    Args:
        occurrences_df: DataFrame with occurrences data
        start_date: Start date in YYYY-MM-DD format
        occ_output_path: Path to existing occurrences CSV file (if exists)
    
    Returns:
        Cleaned and prepared occurrences DataFrame
    """
    if occurrences_df.empty:
        print("No occurrences data to clean")
        return occurrences_df
    
    # Clean occurrences to remove rows earlier than the requested start_date
    try:
        occurrences_df = clean_occurrences(occurrences_df, start_date)
    except Exception as e:
        print(f"Warning: clean_occurrences failed: {e} (continuing without date-based filtering)")
    
    if occurrences_df.empty:
        print("No data after cleaning")
        return occurrences_df
    
    # If file exists, append new data (avoid duplicates)
    occurrences_to_save = occurrences_df
    if os.path.exists(occ_output_path):
        existing_df = pd.read_csv(occ_output_path)
        # Combine and remove duplicates based on event_id and date
        combined_df = pd.concat([existing_df, occurrences_df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(
            subset=['event_id', 'date'],
            keep='last'
        )
        occurrences_to_save = combined_df
    
    # Filter to only keep rows with year 2024 or 2025
    print("Filtering data to years 2024 and 2025 only...")
    
    # Ensure date column is datetime for year extraction
    occurrences_to_save['date'] = pd.to_datetime(occurrences_to_save['date'], errors='coerce')
    occurrences_to_save['year'] = occurrences_to_save['date'].dt.year
    occurrences_to_save = occurrences_to_save[occurrences_to_save['year'].isin([2024, 2025])].copy()
    occurrences_to_save = occurrences_to_save.drop('year', axis=1)  # Remove temporary year column
    
    print(f"Occurrences after year filtering: {len(occurrences_to_save)} rows")
    
    # Drop duplicate rows (general deduplication)
    before_dedup = len(occurrences_to_save)
    occurrences_to_save = occurrences_to_save.drop_duplicates()
    after_dedup = len(occurrences_to_save)
    if before_dedup != after_dedup:
        print(f"Dropped {before_dedup - after_dedup} duplicate rows")
    
    return occurrences_to_save


def main(start_date: str = "2020-01-01", end_date: str = None):
    """
    Main function to run the data pipeline.
    
    Args:
        start_date: Start date in YYYY-MM-DD format (default: 2020-01-01)
        end_date: End date in YYYY-MM-DD format (default: yesterday)
    """
    # Default to 2020-01-01 and yesterday for historical data extraction
    if end_date is None:
        end_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Extract data
    api_data = extract_events(start_date, end_date)
    
    # Transform data
    events_df, occurrences_df = transform_events(api_data)

    if occurrences_df.empty:
        print("No data to save")
        return
    
    # Save data
    # Choose file names for both tables
    events_output_path = f"data/processed/events.csv"
    occ_output_path = f"data/processed/occurrences.csv"
    
    # Clean and prepare occurrences for saving
    occurrences_to_save = clean_and_prepare_occurrences(occurrences_df, start_date, occ_output_path)
    
    if occurrences_to_save.empty:
        print("No data to save after cleaning")
        return
    
    # Enrich with reverse geocoding (region & country) before saving
    # We'll enrich the final occurrences_to_save later (after optional dedupe)

    # Enrich the occurrences table with reverse geocoding
    try:
        occurrences_to_save = enrich_with_reverse_geocoding(occurrences_to_save)
    except Exception as e:
        print(f"Warning: enrich_with_reverse_geocoding failed: {e} (continuing without region/country)")

    # Save occurrences (geometry level)
    save_data(occurrences_to_save, occ_output_path)

    # events_df already built earlier

    # Augment events metadata with first-occurrence columns from occurrences
    try:
        events_df = add_first_occurrence_to_events(events_df, occurrences_to_save)
    except Exception as e:
        print(f"Warning: failed to add first-occurrence columns to events_df: {e}")

    # Filter events_df to only include events with occurrences in 2024-2025
    # (i.e., events with non-null 'date' column from the filtered occurrences)
    if 'date' in events_df.columns:
        events_df = events_df[events_df['date'].notna()].copy()
        print(f"Events after filtering to 2024-2025 occurrences: {len(events_df)} events")

    # Save events metadata (merge with existing if present)
    try:
        if os.path.exists(events_output_path):
            existing_events = pd.read_csv(events_output_path)
            # If existing events have categories saved as strings, no need to parse
            combined_events = pd.concat([existing_events, events_df], ignore_index=True)
            # Keep latest metadata by event_id
            combined_events = combined_events.drop_duplicates(subset=['event_id'], keep='last')
            save_data(combined_events, events_output_path)
        else:
            save_data(events_df, events_output_path)
    except Exception as e:
        print(f"Warning: failed to save events metadata: {e}")



if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='NASA EONET Data Pipeline')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    main(start_date=args.start_date, end_date=args.end_date)

