import json
from google.cloud import bigquery



def insert_data_into_bigquery(project_id='rsp-data-engineering-ii',dataset_id='eu_disaster',table_id='SevereWeather',file_path ='../data_preprocessing/cleaned.json'):

    client = bigquery.Client(project_id)

    rows = []

    with open(file_path, 'r') as f:
        for line in f:
            try:
                row = json.loads(line)
                rows.append(list(row.values()))
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON: {e}")

    if rows:
        dataset = client.dataset(dataset_id)
        table_ref = dataset.table(table_id)
        table = client.get_table(table_ref)

        errors = client.insert_rows(table, rows)
        if errors:
            print(errors)
        else:
            print('Data inserted successfully.')
    else:
        print('No valid data to insert.')

