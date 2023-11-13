import json
import requests
import csv
import cleaning as cl

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

def save_to_csv(data, filename):
    try:
        # Assuming each item in the 'results' list is a dictionary
        keys = data[0].keys()

        with open(filename, mode='w', newline='', encoding='utf-8') as csv_file:
            writer = csv.DictWriter(csv_file, fieldnames=keys)
            writer.writeheader()
            writer.writerows(data)

        print(f"Data has been saved to {filename}")
    except Exception as e:
        print(f"An unexpected error occurred: {e}")

def job():
    results = extract_data()

    if results:
        try:
            csv_filename = "events_data.csv"
            save_to_csv(results, csv_filename)

        except Exception as e:
            print(f"An unexpected error occurred: {e}")

job()
cl.clean_and_save_data()
