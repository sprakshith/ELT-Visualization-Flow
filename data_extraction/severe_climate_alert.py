import requests

ACCESS_TOKEN = '{Add Your API KEY}'

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

    if response.status_code == 200 :
        data = response.json()
        print(data)
    else:
        print(f"Request failed with status code: {response.status_code}")

except requests.exceptions.RequestException as e:
    print(f"An error occurred while making the request: {e}")

except Exception as e:
    print(f"An unexpected error occurred: {e}")

