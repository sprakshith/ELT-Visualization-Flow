import os
import time
import pandas as pd
import requests_cache
import openmeteo_requests
from retry_requests import retry
from data_loading.load_to_bigquery import create_hist_temp_table

dir_path = os.path.dirname(os.path.realpath(__file__))

cache_session = requests_cache.CachedSession('.cache', expire_after = -1)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

weather_variables = [
    "temperature_2m", 
    "apparent_temperature",
    "relative_humidity_2m",
    "dew_point_2m",
    "wind_speed_10m", 
    "wind_gusts_10m",
    "pressure_msl",
    "surface_pressure",
    "cloud_cover",
    "precipitation",
    "rain",
    "snowfall",
    "snow_depth"
]


def fetch_weather_data(disaster_num, location, latitude, longitude, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"

    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": weather_variables
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    for i, v in enumerate(weather_variables):
        hourly_data[v] = hourly.Variables(i).ValuesAsNumpy()

    hourly_data['disaster_num'] = disaster_num
    hourly_data['location'] = location
    hourly_data['latitude'] = latitude
    hourly_data['longitude'] = longitude

    file_name = f'{disaster_num}_{int(time.time())}.csv'
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    hourly_dataframe.to_csv(f'./Datasets/IntermediateDatasets/WeatherCsvFiles/{file_name}', sep='|', index=False)


def fetch_current_weather_data(disaster_num, location, latitude, longitude, start_date, end_date):
    url = "https://archive-api.open-meteo.com/v1/archive"
    
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "start_date": start_date,
        "end_date": end_date,
        "hourly": weather_variables
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]
    hourly = response.Hourly()

    hourly_data = {"date": pd.date_range(
        start = pd.to_datetime(hourly.Time(), unit = "s"),
        end = pd.to_datetime(hourly.TimeEnd(), unit = "s"),
        freq = pd.Timedelta(seconds = hourly.Interval()),
        inclusive = "left"
    )}

    for i, v in enumerate(weather_variables):
        hourly_data[v] = hourly.Variables(i).ValuesAsNumpy()

    hourly_data['disaster_num'] = disaster_num
    hourly_data['location'] = location
    hourly_data['latitude'] = latitude
    hourly_data['longitude'] = longitude

    file_name = f'{disaster_num}_{int(time.time())}.csv'
    hourly_dataframe = pd.DataFrame(data=hourly_data)
    file_path = os.path.join(dir_path, f'./Datasets/IntermediateDatasets/WeatherCsvFiles/{file_name}')
    hourly_dataframe.to_csv(file_path, sep='|', index=False)


def save_extracted_data(disaster_row):
    disaster_num = disaster_row['DisasterNum']
    location = disaster_row['Location']
    latitude = disaster_row['Latitude']
    longitude = disaster_row['Longitude']
    
    if disaster_row['Subtype'] == 'Heat wave':
        start_date = str(disaster_row['StartYear']) + '-05-01'
        end_date = str(disaster_row['StartYear']) + '-08-31'
    elif disaster_row['Subtype'] == 'Cold wave':
        start_date = str(disaster_row['StartYear']) + '-11-01'
        end_date = str(disaster_row['StartYear']+1) + '-02-28'

    fetch_weather_data(disaster_num, location, latitude, longitude, start_date, end_date)


def load_hist_data_to_bq():
    disaster = pd.read_csv('./Datasets/CleanedDatasets/Disaster.csv', sep='|')
    disaster_clf = pd.read_csv('./Datasets/CleanedDatasets/DisasterClassification.csv', sep='|')
    location_df = pd.read_csv('./Datasets/CleanedDatasets/Location.csv', sep='|')

    merged_df = pd.merge(disaster, disaster_clf, on='ClassificationKey', how='inner')
    merged_df = merged_df[merged_df['Type'] == 'Extreme temperature']
    merged_df = pd.merge(merged_df, location_df, left_on='DisasterNum', right_on='DisasterNo', how='inner')

    merged_df['StartMonth'].fillna(int(merged_df['StartMonth'].mean()), inplace=True)
    merged_df['StartDay'].fillna(int(1), inplace=True)
    merged_df['EndMonth'].fillna(int(merged_df['EndMonth'].mean()), inplace=True)
    merged_df['EndDay'].fillna(merged_df['EndMonth'].apply(lambda x: 28 if x == 2 else 30 if x in [4, 6, 9, 11] else 31), inplace=True)

    merged_df['StartDate'] = pd.to_datetime(merged_df['StartYear'].apply(lambda x: str(int(x))) + '-' + merged_df['StartMonth'].apply(lambda x: str(int(x))) + '-' + merged_df['StartDay'].apply(lambda x: str(int(x))), format='%Y-%m-%d')
    merged_df['EndDate'] = pd.to_datetime(merged_df['EndYear'].apply(lambda x: str(int(x))) + '-' + merged_df['EndMonth'].apply(lambda x: str(int(x))) + '-' + merged_df['EndDay'].apply(lambda x: str(int(x))), format='%Y-%m-%d')

    total_rows = merged_df.shape[0]
    
    for row in merged_df.iterrows():
        try:
            save_extracted_data(row[1])
            print(f'{row[0]+1}/{total_rows} Finished.')
            time.sleep(5)
        except Exception as e:
            print(e)
            print(f'{row[0]+1}/{total_rows} Failed.')
            time.sleep(5)
            if 'Hourly API request limit exceeded' in str(e):
                time.sleep(3600)

    create_hist_temp_table()

if __name__ == '__main__':
    load_hist_data_to_bq()
