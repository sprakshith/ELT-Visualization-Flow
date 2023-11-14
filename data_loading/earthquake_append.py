import os
from google.cloud import bigquery

dir_path = os.path.dirname(os.path.realpath(__file__))
ACCESS_KEY_PATH = os.path.join(dir_path, r"../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ACCESS_KEY_PATH


def insert_earthquake(json_data):
    project_id = "rsp-data-engineering-ii"
    dataset_id = 'eu_disaster'
    table_id = 'Earthquake'

    client = bigquery.Client(project=project_id)
    table_ref = client.get_dataset(dataset_id).table(table_id)
    row_to_insert = [json_data]
    errors = client.insert_rows_json(table_ref, row_to_insert)
    if not errors:
        print("New row has been added.")
    else:
        print("Encountered errors while inserting row: {}".format(errors))
