from data_loading import load_to_bigquery
from data_preprocessing import data_segregation
from data_extraction.weather_data_extraction import load_hist_data_to_bq
from data_extraction.every_earthquake import extract_load_earthquake_data

# The below function call will fetch all the Lat and Lon where the Extreme weather events have occured.
# And then open the openmeteo API to fetch the weather data around the time when the event occured.
# This data is then loaded to BigQuery.
load_hist_data_to_bq()

# The below function call will Clean, Extract Mission Locations(Lat and Lon) using OpenAI and GoogleMaps GeoCoding API.
FILL_MISSING_LOCATION = False  # This runs OpenAI API - Takes a lot of time to complete
FETCH_LAT_LON = False  # This runs Google Maps API - Which Costs Money
RECREATE_TABLES = False  # This will Trucate the tables in the BigQuery and Recreate them. If there is no change, set it to False

data_segregation.segregate_raw_dataset(FILL_MISSING_LOCATION, FETCH_LAT_LON)
load_to_bigquery.create_datasets_and_tables(RECREATE_TABLES)

# The below function will fetch all the latest Earthquakes past month and load it to BigQuery.
# This function will be set to run once a day using Cron Job.
extract_load_earthquake_data()
