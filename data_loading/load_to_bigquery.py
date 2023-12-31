import os
import time
from google.cloud import bigquery

dir_path = os.path.dirname(os.path.realpath(__file__))

credentials_path = os.path.join(dir_path, "../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path


def create_datasets_and_tables(recreate_tables):
    if not recreate_tables:
        return

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
        if table == "LocationClusters" or table == "LocationClustersConnection":
            table_id = f"{dataset_id}.{table}"
            table_file_path = os.path.join(dir_path, f"../Datasets/CleanedDatasets/{table}.csv")

            with open(table_file_path, "rb") as source_file:
                client.load_table_from_file(source_file, table_id, job_config=job_config)


def create_hist_temp_table():
    client = bigquery.Client()

    dataset_id = f"{client.project}.eu_disaster"

    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset, timeout=30)

    datasets_dir_path = os.path.join(dir_path, ".../Datasets/IntermediateDatasets/WeatherCsvFiles/")
    table_names = [file_name.split(".")[0] for file_name in os.listdir(datasets_dir_path)]

    table_id = f"{dataset_id}.HistoricalExtTemp"

    for i, table in enumerate(table_names):
        table_file_path = os.path.join(dir_path, f"../Datasets/IntermediateDatasets/WeatherCsvFiles/{table}.csv")

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
                                            autodetect=True,
                                            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE if i == 0 else bigquery.WriteDisposition.WRITE_APPEND)

        with open(table_file_path, "rb") as source_file:
            client.load_table_from_file(source_file, table_id, job_config=job_config)


def create_forecast_temp_table():
    client = bigquery.Client()

    dataset_id = f"{client.project}.eu_disaster"

    try:
        client.get_dataset(dataset_id)
    except:
        dataset = bigquery.Dataset(dataset_id)
        client.create_dataset(dataset, timeout=30)

    datasets_dir_path = os.path.join(dir_path, "../Datasets/IntermediateDatasets/WeatherForecastCsvFiles/")
    table_names = [file_name.split(".")[0] for file_name in os.listdir(datasets_dir_path)]

    table_id = f"{dataset_id}.ForecastTemp"

    for i, table in enumerate(table_names):
        table_file_path = os.path.join(dir_path,
                                       f"../Datasets/IntermediateDatasets/WeatherForecastCsvFiles/{table}.csv")

        job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.CSV, skip_leading_rows=1,
                                            autodetect=True,
                                            write_disposition=bigquery.WriteDisposition.WRITE_APPEND)

        with open(table_file_path, "rb") as source_file:
            try:
                client.load_table_from_file(source_file, table_id, job_config=job_config)
            except Exception as e:
                print(e)


def load_earthquake_to_bigquery(json_data):
    project_id = "rsp-data-engineering-ii"
    dataset_id = 'eu_disaster'
    table_id = 'Earthquake'

    client = bigquery.Client(project=project_id)
    table_ref = client.get_dataset(dataset_id).table(table_id)

    if type(json_data) == dict:
        row_to_insert = [json_data]
    else:
        row_to_insert = json_data

    errors = client.insert_rows_json(table_ref, row_to_insert)
    if not errors:
        print("New rows have been added.")
    else:
        print("Encountered errors while inserting row: {}".format(errors))


def remove_loaded_csv_files():
    datasets_dir_path = os.path.join(dir_path, "../Datasets/IntermediateDatasets/WeatherForecastCsvFiles/")
    table_names = [file_name.split(".")[0] for file_name in os.listdir(datasets_dir_path)]

    for i, table in enumerate(table_names):
        table_file_path = os.path.join(dir_path,
                                       f"../Datasets/IntermediateDatasets/WeatherForecastCsvFiles/{table}.csv")

        if os.path.exists(table_file_path):
            os.remove(table_file_path)


def load_extreme_temp_locations_forecast():
    create_forecast_temp_table()
    time.sleep(5)
    remove_loaded_csv_files()


load_extreme_temp_locations_forecast()
