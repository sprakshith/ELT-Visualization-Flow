from data_loading import load_to_bigquery
from data_preprocessing import data_segregation


FILL_MISSING_LOCATION = False  # This runs OpenAI API - Takes a lot of time to complete
FETCH_LAT_LON = False  # This runs Google Maps API - Which Costs Money
RECREATE_TABLES = True  # This will Trucate the tables in the BigQuery and Recreate them. If no change set to False.

data_segregation.segregate_raw_dataset(FILL_MISSING_LOCATION, FETCH_LAT_LON)
load_to_bigquery.create_datasets_and_tables(RECREATE_TABLES)
