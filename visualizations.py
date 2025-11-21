#!/usr/bin/env python3
"""
NASA EONET Data Visualization Script

This script creates and saves Altair-Vega visualizations for natural events
data from NASA EONET.
"""

import pandas as pd
import altair as alt
from pathlib import Path
import os
import sys

# Add src directory to path to import from file
sys.path.insert(0, 'src')

from charts import create_daily_chart, create_big_number_chart, create_radial_chart, create_geospatial_map

# Configuration
output_dir = "graphs"
data_dir = "data/processed"

# Ensure output directory exists
os.makedirs(output_dir, exist_ok=True)

# ============================================================================
# LOAD DATA
# ============================================================================
print("Loading data...")
events_df = pd.read_csv(f"{data_dir}/events.csv")
occurrences_df = pd.read_csv(f"{data_dir}/occurrences.csv")

# Convert date columns to datetime
events_df['date'] = pd.to_datetime(events_df['date'])
occurrences_df['date'] = pd.to_datetime(occurrences_df['date'])

print(f"✓ Loaded {len(events_df)} events and {len(occurrences_df)} occurrences")
print()

# ============================================================================
# CHART 1: DAILY EVENT COUNT CHART
# ============================================================================
print("=" * 80)
print("CHART 1: Daily Event Count Chart")
print("=" * 80)
print("Creating line chart showing the number of events per day...")

daily_chart = create_daily_chart(occurrences_df)

# Save to HTML
daily_chart.save(f"{output_dir}/daily_event_count.html")
print(f"✓ Saved daily event count chart to {output_dir}/daily_event_count.html")
print()

# ============================================================================
# CHART 2: BIG NUMBER CHART
# ============================================================================
print("=" * 80)
print("CHART 2: Big Number Chart")
print("=" * 80)
print("Creating total event count display...")

big_number_chart = create_big_number_chart(events_df)

# Save to HTML
big_number_chart.save(f"{output_dir}/total_nasa_events_big_number.html")
print(f"✓ Saved big number chart to {output_dir}/total_nasa_events_big_number.html")
print()

# ============================================================================
# CHART 3: RADIAL CHART
# ============================================================================
print("=" * 80)
print("CHART 3: Radial Chart")
print("=" * 80)
print("Creating radial/donut chart showing event distribution by category...")

radial_chart = create_radial_chart(events_df)

# Save to HTML
radial_chart.save(f"{output_dir}/nasa_events_radial_chart.html")
print(f"✓ Saved radial chart to {output_dir}/nasa_events_radial_chart.html")
print()

# ============================================================================
# CHART 4: GEOSPATIAL EVENT MAP
# ============================================================================
print("=" * 80)
print("CHART 4: Geospatial Event Map")
print("=" * 80)
print("Creating world map showing geographic distribution of events...")

geo_map = create_geospatial_map(occurrences_df)

# Save to HTML
geo_map.save(f"{output_dir}/geospatial_event_map.html")
print(f"✓ Saved geospatial map to {output_dir}/geospatial_event_map.html")
print()

# ============================================================================
# SUMMARY
# ============================================================================
print("=" * 80)
print("VISUALIZATION GENERATION COMPLETE")
print("=" * 80)
print(f"✓ All charts saved to: {output_dir}/")
print()
print("Generated files:")
print(f"  1. daily_event_count.html")
print(f"  2. total_nasa_events_big_number.html")
print(f"  3. nasa_events_radial_chart.html")
print(f"  4. geospatial_event_map.html")
print()
