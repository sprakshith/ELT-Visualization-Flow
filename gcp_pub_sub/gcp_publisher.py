import os
import json
from google.cloud import pubsub_v1

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json"


def publish(earthquake_data):
    publisher = pubsub_v1.PublisherClient()

    project_id = "rsp-data-engineering-ii"
    topic_name = "EarthquakeTopic"

    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        earthquake_data = earthquake_data.encode("utf-8")
        future = publisher.publish(topic_path, data=earthquake_data)
        print('Data Published: ', future.result())
    except Exception as e:
        print(e)


# TODO:
#   Instead of the dummy file we need to write a code to extract earthquakes from the API
#   and then publish it to the topic.
file_path = 'dummyEarthquake.json'

try:
    with open(file_path, 'r') as file:
        data = file.read()
        publish(data)
except FileNotFoundError:
    print(f"The file {file_path} was not found.")
except json.JSONDecodeError:
    print(f"Error decoding JSON from the file {file_path}.")
