import openmeteo_requests
import requests_cache
import pandas as pd
from retry_requests import retry
import time
import json

url = "https://flood-api.open-meteo.com/v1/flood"

def fetch_and_save_api_data(csv_file_path, output_json_path='output_data.json', delay=1):
    cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
    retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
    openmeteo = openmeteo_requests.Client(session=retry_session)

    df = pd.read_csv(csv_file_path)

    all_location_responses = []

    for index, row in df.iterrows():

        latitude = row['Latitude']
        longitude = row['Longitude']
        params = {
            "latitude": latitude,
            "longitude": longitude,
            "daily": "river_discharge"
        }

        responses = openmeteo.weather_api(url, params=params)
        if responses:
            response = responses[0]
            location_data = {
                "latitude": response.Latitude(),
                "longitude": response.Longitude(),
                "elevation": response.Elevation(),
                "timezone": response.Timezone(),
                "timezone_abbreviation": response.TimezoneAbbreviation(),
                "utc_offset_seconds": response.UtcOffsetSeconds(),
                "daily_data": {
                    "date": pd.date_range(
                        start=pd.to_datetime(response.Daily().Time(), unit="s"),
                        end=pd.to_datetime(response.Daily().TimeEnd(), unit="s"),
                        freq=pd.Timedelta(seconds=response.Daily().Interval()),
                        inclusive="left"
                    ).strftime('%Y-%m-%d %H:%M:%S').tolist(),  # Convert to string and then to list
                    "river_discharge": response.Daily().Variables(0).ValuesAsNumpy().tolist()

                }
            }

            all_location_responses.append(location_data)
        else:
            print(f"No response for latitude {latitude}, longitude {longitude}")

        time.sleep(delay)

    df_output = pd.DataFrame(all_location_responses)

    df_output.rename(columns={'data': 'rdata'}, inplace=True)

    df_output.to_json(output_json_path, orient='records', indent=2)

    print("File created")

fetch_and_save_api_data('location1.csv', output_json_path='output_data.json', delay=1)
