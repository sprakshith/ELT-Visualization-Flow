
import pandas as pd

project_id='rsp-data-engineering-ii'
dataset_id='eu_disaster'
table_id='FloodAlert'
json_file_path='../data_preprocessing/flood_output_file.json'
df = pd.read_json(json_file_path)

# Convert 'rdate' column to datetime
df['rdate'] = pd.to_datetime(df['rdate'], errors='coerce')
# Specify the BigQuery table schema
schema = [
    {"name": "latitude", "type": "FLOAT64"},
    {"name": "longitude", "type": "FLOAT64"},
    {"name": "rdate", "type": "DATETIME"},
    {"name": "river_discharge", "type": "FLOAT64"},
    {"name": "elevation", "type": "FLOAT64"},
    {"name": "timezone", "type": "STRING"},
    {"name": "timezone_abbreviation", "type": "STRING"}
]

# Upload the DataFrame to BigQuery
df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{table_id}',
         project_id=project_id,
         if_exists='replace',
         table_schema=schema)

