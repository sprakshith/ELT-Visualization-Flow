import json

response = json.loads(open('past_every_earthquake.json', 'r').read())

# print(response['data'][0].keys())

# ids = [data['id'][:2] for data in response['data'] if data['id'][:2] == 'pr']
ids = [data['id'][:2] for data in response['data']]

for code in set(ids):
    cds = [data for data in response['data'] if data['id'][:2] == code]
    print(f"{cds[0]['country']}, {cds[0]['continent']}, {code}")
