


I’d model EONET events with two DataFrames, not one:
	1.	events_df → 1 row = 1 event (metadata)
	2.	occurrences_df → 1 row = 1 geometry/occurrence (time + coordinates), with event_id as a foreign key

This matches the conceptual structure of the API (event vs. geometries) and keeps time-varying data separate from static metadata. NASA’s EONET documentation explicitly structures each event with a list of geometries over time, i.e. one-to-many.


events_df
columns:

 "event_id": ev.get("id"),
        "title": ev.get("title"),
        "description": ev.get("description"),
        "link": ev.get("link"),
        "categories": [c["title"] for c in ev.get("categories", [])],
        "sources": [s["id"] for s in ev.get("sources", [])],
        "closed": ev.get("closed"),
        "status": ev.get("status"),


occurrences_df (geometry-level table)

