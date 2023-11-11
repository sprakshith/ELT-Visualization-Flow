import json
import requests

url = "https://everyearthquake.p.rapidapi.com/2.5_month.json"

headers = {
    "X-RapidAPI-Key": "8b11e016dbmsh4cbcc7882a937b8p18a64djsn48a0a59cc2c9",
    "X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
}

response = requests.get(url, headers=headers)

with open('past_every_earthquake.json', 'w') as file:
    file.write(json.dumps(response.json()))
