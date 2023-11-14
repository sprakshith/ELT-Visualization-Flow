import os
import json
from google.cloud import pubsub_v1
from data_loading.earthquake_append import insert_earthquake

dir_path = os.path.dirname(os.path.realpath(__file__))
ACCESS_KEY_PATH = os.path.join(dir_path, r"../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json")
os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = ACCESS_KEY_PATH

project_id = "rsp-data-engineering-ii"
subscription_name = "DisastersSubscription"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)


def callback(message):
    message_string = message.data.decode("utf-8")
    message_dict = json.loads(message_string)

    if message_dict['extracted_data_type'] == 'earthquake':
        insert_earthquake(message_dict)
        message.ack()
        return


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
        start_subscriber()


if __name__ == '__main__':
    start_subscriber()
