# Streamlit Cloud Deployment Guide

This guide explains how to deploy the NASA Natural Events app to Streamlit Cloud.

## Prerequisites

1. A GitHub account
2. Your code pushed to a GitHub repository
3. A Streamlit Cloud account (free at [share.streamlit.io](https://share.streamlit.io))

## Deployment Steps

### 1. Push Code to GitHub

Ensure your code is pushed to a GitHub repository:

```bash
git add .
git commit -m "Prepare for Streamlit Cloud deployment"
git push origin main
```

### 2. Deploy to Streamlit Cloud

1. Go to [share.streamlit.io](https://share.streamlit.io)
2. Sign in with your GitHub account
3. Click "New app"
4. Select your repository
5. Set the main file path to: `streamlit_app.py`
6. Set the branch to: `main` (or your default branch)
7. Click "Deploy"

### 3. Configuration

The app is configured via `.streamlit/config.toml`:
- Theme colors
- Server settings
- Browser settings

### 4. Environment Variables (if needed)

If you need environment variables:
1. Go to your app settings in Streamlit Cloud
2. Click "Advanced settings"
3. Add environment variables as needed

## Project Structure

```
nasa-natural-events/
├── streamlit_app.py      # Main Streamlit app (entry point)
├── app.py                # Alternative entry point
├── requirements.txt      # Python dependencies
├── pyproject.toml        # Project metadata (for uv)
├── .streamlit/
│   └── config.toml       # Streamlit configuration
├── src/
│   ├── __init__.py
│   ├── data_pipeline.py  # Data extraction and transformation
│   └── charts.py        # Chart creation functions
└── README.md
```

## Troubleshooting

### Import Errors

If you see import errors, ensure:
- All dependencies are in `requirements.txt`
- The `src/` package structure is correct
- `src/__init__.py` exists

### API Rate Limits

The app fetches data from NASA EONET API. If you encounter rate limits:
- The app uses caching (`@st.cache_data`) to reduce API calls
- Consider implementing a data refresh schedule

### Memory Issues

If the app runs out of memory:
- Reduce the date range in `load_events_data()`
- Optimize data processing in `data_pipeline.py`

## Updating the App

After making changes:
1. Commit and push to GitHub
2. Streamlit Cloud will automatically redeploy
3. Check the logs in Streamlit Cloud dashboard for any errors

