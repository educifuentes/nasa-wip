# 🌍 NASA EONET Data Pipeline Project

A data pipeline that extracts, transforms, and loads natural events data from the NASA EONET API, enabling analysis using Python (pandas) and DuckDB.

## Project Structure

```
nasa-events-new/
├── data/
│   ├── processed/          # Processed CSV files
│   └── raw/                 # Raw API responses (JSON)
├── graphs/                  # Generated visualizations
├── notebooks/
│   └── exploratory_analysis.ipynb  # DuckDB analysis notebook
├── data_pipeline.py         # Main data extraction and transformation script
├── visualizations.py        # Visualization generation script
├── run_daily.py             # Daily automation script
├── requirements.txt         # Python dependencies
└── README.md               # This file
```

## Setup

1. **Install dependencies:**
   ```bash
   pip install -r requirements.txt
   ```

2. **Create necessary directories:**
   ```bash
   mkdir -p data/processed data/raw graphs notebooks
   ```

## Usage

### Step 1: Data Pipeline

Extract and transform data from NASA EONET API:

```bash
# Extract data for a specific date range
python data_pipeline.py --start-date 2024-01-01 --end-date 2024-12-31

# Extract data for yesterday to today (default for daily automation)
python data_pipeline.py
```

The script will:
- Extract events from NASA EONET API V3
- Transform and clean the data
- Save processed data to `data/processed/events_YYYY.csv`
- Save raw API response to `data/raw/`

### Step 2: Exploratory Analysis

Open the Jupyter notebook to run DuckDB queries:

```bash
jupyter notebook notebooks/exploratory_analysis.ipynb
```

The notebook contains queries to answer:
1. Most frequent event category in 2024
2. Distribution of events by category
3. Occurrences per month
4. Week with most occurrences
5. Month with most events per category
6. Geographic distribution of events

### Step 3: Data Visualization

Generate visualizations using Altair:

```bash
python visualizations.py --data-path data/processed/events_2024.csv
```

This will create:
- Daily event count chart (line chart)
- Geospatial event map (world map with event locations)

Visualizations are saved in the `graphs/` folder as both HTML and PNG files.

## Daily Automation

Run the complete pipeline daily:

```bash
python run_daily.py
```

This script:
1. Extracts data for the current day
2. Transforms and stores the data
3. Updates visualizations

### Setting up a Cron Job (Linux/Mac)

To run the pipeline automatically every day, add a cron job:

```bash
# Edit crontab
crontab -e

# Add this line to run daily at 2 AM
0 2 * * * cd /path/to/nasa-events-new && /usr/bin/python3 run_daily.py >> logs/daily_run.log 2>&1
```

### Setting up Task Scheduler (Windows)

1. Open Task Scheduler
2. Create a new task
3. Set trigger to "Daily"
4. Set action to run: `python run_daily.py`
5. Set working directory to the project folder

## API Documentation

- NASA EONET API V3: [https://eonet.gsfc.nasa.gov/docs/v3](https://eonet.gsfc.nasa.gov/docs/v3)
- API Endpoint: `https://eonet.gsfc.nasa.gov/api/v3/events?start={start_date}&end={end_date}`

## Data Schema

The processed CSV contains the following columns:
- `event_id`: Unique event identifier
- `event_title`: Event title/name
- `event_description`: Event description
- `event_link`: Link to event details
- `category_ids`: Comma-separated category IDs
- `category_titles`: Comma-separated category names
- `occurrence_date`: Date and time of occurrence
- `occurrence_type`: Type of geometry/occurrence
- `longitude`: Longitude coordinate
- `latitude`: Latitude coordinate
- `year`: Extracted year
- `month`: Extracted month
- `day`: Extracted day

## Notes

- The pipeline handles duplicate events by keeping the most recent occurrence
- Geographic analysis uses coordinate-based regions (reverse geocoding can be added for country-level analysis)
- The visualization script requires valid geographic coordinates for the map visualization

## License

This project is for educational and research purposes.

