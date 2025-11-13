"""
Daily Automation Script for NASA EONET Data Pipeline
Runs the complete pipeline: extraction, transformation, storage, and visualization
"""

import subprocess
import sys
from datetime import datetime, timedelta
import os


def run_command(command, description):
    """
    Run a shell command and handle errors.
    
    Args:
        command: Command to run
        description: Description of what the command does
    """
    print(f"\n{'='*60}")
    print(f"Running: {description}")
    print(f"Command: {command}")
    print(f"{'='*60}\n")
    
    try:
        result = subprocess.run(
            command,
            shell=True,
            check=True,
            capture_output=True,
            text=True
        )
        print(result.stdout)
        if result.stderr:
            print("Warnings/Errors:", result.stderr)
        return True
    except subprocess.CalledProcessError as e:
        print(f"Error running {description}:")
        print(f"Return code: {e.returncode}")
        print(f"Error output: {e.stderr}")
        return False


def main():
    """
    Main function to run the daily pipeline.
    """
    print("="*60)
    print("NASA EONET Daily Data Pipeline")
    print(f"Execution started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)
    
    # Get date range (yesterday to today for daily updates)
    end_date = datetime.now().strftime('%Y-%m-%d')
    start_date = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
    
    print(f"\nDate range: {start_date} to {end_date}")
    
    # Step 1: Data extraction and transformation
    pipeline_cmd = f"python data_pipeline.py --start-date {start_date} --end-date {end_date}"
    if not run_command(pipeline_cmd, "Data Pipeline (Extraction & Transformation)"):
        print("Pipeline failed. Stopping execution.")
        sys.exit(1)
    
    # Step 2: Generate visualizations
    # Determine the year for the data file
    year = datetime.now().year
    data_path = f"data/processed/events_{year}.csv"
    
    if os.path.exists(data_path):
        viz_cmd = f"python visualizations.py --data-path {data_path}"
        if not run_command(viz_cmd, "Data Visualization"):
            print("Visualization generation failed, but continuing...")
    else:
        print(f"Warning: Data file {data_path} not found. Skipping visualization.")
    
    print("\n" + "="*60)
    print("Daily pipeline execution completed!")
    print(f"Execution finished at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("="*60)


if __name__ == "__main__":
    main()


