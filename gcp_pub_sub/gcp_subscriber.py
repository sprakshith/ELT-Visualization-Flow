import os
import json
from google.cloud import pubsub_v1
from data_loading.load_to_bigquery import load_earthquake_to_bigquery

dir_path = os.path.dirname(os.path.realpath(__file__))
ACCESS_KEY_PATH = os.path.join(dir_path, r"../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ACCESS_KEY_PATH

project_id = "rsp-data-engineering-ii"
subscription_name = "DisastersSubscription"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)


def callback(message):
    try:
        message_string = message.data.decode("utf-8")
        message_dict = json.loads(message_string)
        print(message_dict)
        if message_dict['extracted_data_type'] == 'earthquake':
            load_earthquake_to_bigquery(message_dict)
            return
        else:
            message.ack()
    except Exception as e:
        print(e)
        message.ack()


def start_subscriber():
    streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)
    try:
        print('Listening!')
        streaming_pull_future.result()
    except KeyboardInterrupt:
        print('Keyboard Interruption!')
        streaming_pull_future.cancel()
    except Exception as e:
        print(e)
        streaming_pull_future.cancel()


if __name__ == '__main__':
    start_subscriber()
