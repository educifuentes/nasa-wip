# 🌍 NASA EONET Data Pipeline Project

## **Context**

You are part of an organization that monitors **natural events globally**.  
Your goal is to build a **data pipeline** that extracts, transforms, and loads public data on **natural events** from the **NASA Open API (EONET)**, enabling its analysis using **Python (pandas)** and **DuckDB**.

The analytics team needs to answer key questions about trends, frequency, and geographic distribution of these events.  
This process must also be **automated to run daily**.

---

## **Step 1 — Build a Data Pipeline (Python Script)**

Create a Python script that performs the following tasks:

### **1. Data Extraction**
- Extract **natural events** from the **NASA EONET API (V3)** within a given date range.  
  - **Documentation:** [https://eonet.gsfc.nasa.gov/docs/v3](https://eonet.gsfc.nasa.gov/docs/v3)  
  - **API Endpoint:**  
    [https://eonet.gsfc.nasa.gov/api/v3/events?start={start_date}&end={end_date}](https://eonet.gsfc.nasa.gov/api/v3/events?start=%7Bstart_date%7D&end=%7Bend_date%7D)

### **2. Data Context**
- **Event:** General event information.  
  - Example: *Disaster in Curicó*  
- **Categories:** An event may belong to multiple categories.  
  - Example: *Wildfires, Earthquake*  
- **Geometry / Occurrences:** Specific instances of an event happening at a certain location and time.  
  - Example:  
    - Fire at location X at time A  
    - Fire at location Y at time B  
    - Earthquake at location Z at time C  

### **3. Data Transformation**
- Clean and transform the extracted data for further analysis.
- Save the preprocessed dataset locally (e.g., `data/processed/events_2024.csv`).

---

## **Step 2 — Exploratory Analysis (Jupyter Notebook using DuckDB)**

In a **Jupyter Notebook**, use **DuckDB** to query the preprocessed data.  
Create separate code blocks for each question below:

1. What was the most frequent event category in 2024?  
2. What was the distribution of events by category in 2024?  
3. How many occurrences were there per month in 2024?  
4. Which week had the most occurrences in 2024?  
5. For each category, which month had the most events?  
6. In which countries or continents were the largest number of events concentrated in 2024?  

---

## **Step 3 — Data Visualization (Python Script using Altair-Vega)**

Create a separate **Python script** that builds visualizations using the **Altair-Vega** library.

### **Visualizations to Include**
- **Daily Event Count Chart:**  
  A line or bar chart showing the number of events per day.  
- **Geospatial Event Map:**  
  A world map with event locations plotted as points.  
  - Use this example for reference:  
    [Altair Geographic Plots Tutorial](https://altair-viz.github.io/altair-tutorial/notebooks/09-Geographic-plots.html)

### **Output**
- Save all generated visualizations in a folder named `graphs/`.

---

## **Automation Requirement**

Ensure the entire process can be automated to run **daily**, performing:
1. Data extraction for the current day  
2. Data preprocessing  
3. Data storage  
4. Visualization updates  

---