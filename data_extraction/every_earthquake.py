import json
import time
import requests
from gcp_pub_sub.gcp_publisher import publish
from access_credentials.rapid_api_key import RAPID_API_KEY
from data_loading.load_to_bigquery import load_earthquake_to_bigquery


def extract_load_earthquake_data(load_all_at_once=False):
    url = "https://everyearthquake.p.rapidapi.com/1.0_day.json"

    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers)
    response_json = response.json()

    cleaned_data = []
    for item in response_json['data']:
        try:
            item['extracted_data_type'] = 'earthquake'
            item['earthquake_id'] = item.pop('id')
            item.pop('locationDetails', None)

            if load_all_at_once:
                cleaned_data.append(item)
            else:
                publish(json.dumps(item))
                time.sleep(1)
        except Exception as e:
            print(e)

    if load_all_at_once:
        try:
            load_earthquake_to_bigquery(cleaned_data)
        except Exception as e:
            print(e)


if __name__ == '__main__':
    extract_load_earthquake_data(load_all_at_once=True)
