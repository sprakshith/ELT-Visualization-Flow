import os
from google.cloud import pubsub_v1

dir_path = os.path.dirname(os.path.realpath(__file__))
ACCESS_KEY_PATH = os.path.join(dir_path, r"../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ACCESS_KEY_PATH


def publish(weather_data):
    publisher = pubsub_v1.PublisherClient()

    project_id = "rsp-data-engineering-ii"
    topic_name = "DisastersTopic"

    topic_path = publisher.topic_path(project_id, topic_name)

    try:
        weather_data = weather_data.encode("utf-8")
        future = publisher.publish(topic_path, data=weather_data)
        print('Data Published: ', future.result())
    except Exception as e:
        print(e)
