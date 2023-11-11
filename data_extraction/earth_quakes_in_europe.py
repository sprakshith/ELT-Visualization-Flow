import json
import requests

url = "https://everyearthquake.p.rapidapi.com/earthquakes"

querystring = {"count": "100", "type": "earthquake", "latitude": "52.5", "longitude": "16",
               "radius": "4000", "units": "kilometers", "magnitude": "2.5", "intensity": "1"}

headers = {
    "X-RapidAPI-Key": "8b11e016dbmsh4cbcc7882a937b8p18a64djsn48a0a59cc2c9",
    "X-RapidAPI-Host": "everyearthquake.p.rapidapi.com"
}

response = requests.get(url, headers=headers, params=querystring)

print(response.json())

with open('past_every_earthquake_in_europe.json', 'w') as file:
    file.write(json.dumps(response.json()))
