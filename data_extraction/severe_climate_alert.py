import json
import requests
from data_preprocessing import severe_weather_preprocessing as swp


ACCESS_TOKEN = '5Xbt14b32p7NW6IK3SShRXVVmwLJ6Czi3dkL6TH2'

def extract_data():
    try:
        response = requests.get(
            url="https://api.predicthq.com/v1/events/",
            headers={
                "Authorization": f"Bearer {ACCESS_TOKEN}",
                "Accept": "application/json"
            },
            params={
                "country": "DE,DK,SE,NL,LU,FR,CH,AT,CZ,PL,ES,IT,RO,GR,PT,HU,UA,RU,RS,SK,FI,NO,IE,HR,BA,AL,SI,LT,LV,EE,ME,MT,IS,AD,LI,MC,SM",
                "category": "severe-weather,disasters"
            }
        )

        if response.status_code == 200:
            data = response.json()

            if 'results' in data:
                results = data['results']
                return results

            else:
                print("No 'results' key in the API response data.")
                return None
        else:
            print(f"Request failed with status code: {response.status_code}")
            return None
    except requests.exceptions.RequestException as e:
        print(f"An error occurred while making the request: {e}")
        return None
    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None

def save_to_json(data, filename):
    try:
        with open(filename, mode='w', encoding='utf-8') as json_file:
            json.dump(data, json_file, ensure_ascii=False, indent=2)

        print(f"Data has been saved to {filename}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def job():
    results = extract_data()

    if results:
        try:
            json_filename = "events_data.json"
            save_to_json(results, json_filename)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

    swp.clean_and_save_data()


if __name__ == '__main__':
    job()


