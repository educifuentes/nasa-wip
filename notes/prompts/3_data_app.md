



Since the data can be obtained with an API, there is no need to read csv.

take the steps to extract and transform data from file [text](../data_pipeline.py).

Extract the data from the last 5 yearsd , with end date yesterdat's date.



---

1


Build a data app using streamlit library.

The docuemnation is here: 

The title of dashboard is Nasa Eevents 2025
subtitle is "Gobal"

Then organize teh content , first in 2 divs, the first 25% and the other 75%.

The first div, divide in 3 equal divs.

- Top: place the one place the daily event altair chart (daily_chart)
- Middle: place the big number altair chart (big_number_chart)
- Bottom: place the radial chart altair chart (radial_chart)

In the second div (75%) place the geospatial event map (map_chart)

use st.altair_chart function to display the charts.

All the charts are in the [text](../notebooks/visualizations.ipynb)

