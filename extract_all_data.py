from data_extraction.every_earthquake import extract_load_earthquake_data
from data_loading.load_to_bigquery import load_extreme_temp_locations_forecast

# Code to Extract Earthquake Data and Load it to BigQuery
try:
    extract_load_earthquake_data()
except Exception as e:
    print(e)

# Code to Extract Weather Data and Load it to BigQuery
try:
    load_extreme_temp_locations_forecast()
except Exception as e:
    print(e)

# Code to Extract Severe Weather Data and Load it to BigQuery
try:
    # TODO: Extract and Load Severe Weather Data
    # Write your code here
    pass
except Exception as e:
    print(e)
