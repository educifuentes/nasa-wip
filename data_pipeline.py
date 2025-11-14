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
try:
    import pycountry_convert as pc
except Exception:
    pc = None


def extract_events(start_date: str, end_date: str) -> dict:
    """
    Extract natural events from NASA EONET API V3 within a date range.
    
    Args:
        start_date: Start date in YYYY-MM-DD format
        end_date: End date in YYYY-MM-DD format
    
    Returns:
        Dictionary containing API response
    """
    base_url = "https://eonet.gsfc.nasa.gov/api/v3/events"
    params = {
        "start": start_date,
        "end": end_date
    }
    
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


def transform_events(api_data: dict) -> pd.DataFrame:
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
                'occurrence_date': None,
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
                    'occurrence_date': date_str,
                    'occurrence_type': geom.get('type', ''),
                    'longitude': longitude,
                    'latitude': latitude
                })
    
    df = pd.DataFrame(transformed_data)
    
    # Data cleaning
    if not df.empty:
        # Convert date column to datetime
        df['occurrence_date'] = pd.to_datetime(df['occurrence_date'], errors='coerce')
        
        # Extract year for filtering
        df['year'] = df['occurrence_date'].dt.year
        df['month'] = df['occurrence_date'].dt.month
        df['day'] = df['occurrence_date'].dt.day
        
        # Remove rows with invalid coordinates (if needed)
        # Keep them for now as they might have other useful data
        
        print(f"Transformed {len(df)} records from {len(events)} events")
    
    return df


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


def main(start_date: str = None, end_date: str = None):
    """
    Main function to run the data pipeline.
    
    Args:
        start_date: Start date in YYYY-MM-DD format (default: yesterday)
        end_date: End date in YYYY-MM-DD format (default: today)
    """
    # Default to yesterday and today for daily automation
    if end_date is None:
        end_date = datetime.now().strftime('%Y-%m-%d')
    if start_date is None:
        start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    # Extract data
    api_data = extract_events(start_date, end_date)
    
    # Transform data
    df = transform_events(api_data)
    
    if df.empty:
        print("No data to save")
        return
    
    # Save data
    year = datetime.now().year
    output_path = f"data/processed/events_{year}.csv"
    
    # If file exists, append new data (avoid duplicates)
    if os.path.exists(output_path):
        existing_df = pd.read_csv(output_path)
        # Combine and remove duplicates based on event_id and occurrence_date
        combined_df = pd.concat([existing_df, df], ignore_index=True)
        combined_df = combined_df.drop_duplicates(
            subset=['event_id', 'occurrence_date'],
            keep='last'
        )
        df = combined_df
    
    # Enrich with reverse geocoding (region & country) before saving
    try:
        df = enrich_with_reverse_geocoding(df)
    except Exception as e:
        print(f"Warning: enrich_with_reverse_geocoding failed: {e} (continuing without region/country)")

    save_data(df, output_path)
    
    # Also save raw API response for reference
    raw_output_path = f"data/raw/events_{start_date}_to_{end_date}.json"
    os.makedirs(os.path.dirname(raw_output_path), exist_ok=True)
    with open(raw_output_path, 'w') as f:
        json.dump(api_data, f, indent=2)
    print(f"Raw API response saved to {raw_output_path}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description='NASA EONET Data Pipeline')
    parser.add_argument('--start-date', type=str, help='Start date (YYYY-MM-DD)')
    parser.add_argument('--end-date', type=str, help='End date (YYYY-MM-DD)')
    
    args = parser.parse_args()
    
    main(start_date=args.start_date, end_date=args.end_date)

