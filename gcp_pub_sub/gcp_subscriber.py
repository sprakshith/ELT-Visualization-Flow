import os
import json
from google.cloud import pubsub_v1
from data_loading.earthquake_append import insert_earthquake

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = r"../access_credentials/rsp-dm-ii-dv-iii-elt-flow.json"

project_id = "rsp-data-engineering-ii"
subscription_name = "EarthquakeSub"

subscriber = pubsub_v1.SubscriberClient()
subscription_path = subscriber.subscription_path(project_id, subscription_name)


def callback(message):
    earthquake_string = message.data.decode("utf-8")
    earthquake_dict = json.loads(earthquake_string)
    print(earthquake_dict)
    insert_earthquake(earthquake_dict)
    message.ack()


streaming_pull_future = subscriber.subscribe(subscription_path, callback=callback)

try:
    print('Listening!')
    streaming_pull_future.result()
except KeyboardInterrupt:
    print('Keyboard Interruption!')
    streaming_pull_future.cancel()
