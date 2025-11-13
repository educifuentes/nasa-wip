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

