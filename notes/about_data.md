


https://eonet.gsfc.nasa.gov/docs/v3


- Each record describes a single natural event, and inside it there is a list of occurrences of that event (called geometries).
- NASA calls the event the overall phenomenon (e.g., a wildfire), and each geometry is one observation of that event at a specific time.


✔️ What is an occurrence of that event?

Inside each event, the field geometry contains one or more observations of the same event at different times.

Each geometry entry includes:
	•	date → timestamp of the observation
	•	type → Point or Polygon
	•	coordinates → location
	•	optional metadata


Thus:
	•	If an event lasts 10 days, you may see 10 geometry items.
	•	If an event expands spatially (e.g., wildfire, a polygon may replace a point over time).




    Data Modelation

    - 	•	Analyze event durations, number of observations, etc. (group by event_id in occurrences_df)
