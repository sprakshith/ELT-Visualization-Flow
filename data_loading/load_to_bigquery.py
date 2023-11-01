import os
from google.cloud import bigquery

dir_path = os.path.dirname(os.path.realpath(__file__))


def create_datasets_and_tables(recreate_tables):
    if not recreate_tables:
        return

    credentials_path = os.path.join(dir_path, "../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json")
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path

    client = bigquery.Client()

    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1, autodetect=True,
                                        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE)

    dataset_id = f"{client.project}.eu_disaster"

    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset, timeout=30)

    datasets_dir_path = os.path.join(dir_path, "../Datasets/CleanedDatasets/")
    table_names = [file_name.split(".")[0] for file_name in os.listdir(datasets_dir_path)]

    for table in table_names:
        table_id = f"{dataset_id}.{table}"
        table_file_path = os.path.join(dir_path, f"../Datasets/CleanedDatasets/{table}.csv")

        with open(table_file_path, "rb") as source_file:
            client.load_table_from_file(source_file, table_id, job_config=job_config)
