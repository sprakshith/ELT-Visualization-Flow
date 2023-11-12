import requests
from data_loading.load_to_bigquery import load_earthquake_to_bigquery
from access_credentials.rapid_api_key import RAPID_API_KEY


def extract_load_earthquake_data():
    url = "https://everyearthquake.p.rapidapi.com/2.5_month.json"

    headers = {
        "X-RapidAPI-Key": RAPID_API_KEY,
        "X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
    }

    response = requests.get(url, headers=headers)
    response_json = response.json()

    cleaned_data = []
    for item in response_json['data']:
        item['earthquake_id'] = item.pop('id')
        item.pop('locationDetails', None)
        cleaned_data.append(item)

    load_earthquake_to_bigquery(cleaned_data)


if __name__ == '__main__':
    extract_load_earthquake_data()
