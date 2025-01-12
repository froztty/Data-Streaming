from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import time

"""This fetches random user API data and streams the data to a Kafka topic (user_created)
   which is done by Apache Airflow. """
default_args = {
    'owner': 'airscholar',
    'start_date': datetime(2024, 12, 30, 10, 00)
}

def get_data():
    import requests

    res = requests.get("http://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]

    return res

def format_data(res):
    data = {}
    location = res['location']
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(res['location']['street']['number'])} {location['street']['name']}, " \
                        f"{location['city']}, {location['state']}, {location['country']}"
    data['postcode'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['registered_date'] = res['registered']['date']
    data['dob'] = res['dob']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']

    return data

def stream_data():
    import json
    from kafka import KafkaProducer

    import logging

    res = get_data()
    res = format_data(res)
    producer = KafkaProducer(bootstrap_servers=['broker:29092'], max_block_ms=5000)
    if not producer:
        logging.error("Failed to connect to Kafka after retries.")
        return

    curr_time = time.time()

    while True:
        if time.time() > curr_time + 60: # 1 minute
            break
        try:
            res = get_data()
            res = format_data(res)

            producer.send('users_created', json.dumps(res).encode('utf-8'))
        except Exception as e:
            logging.error(f'An error occurred for kafka stream: {e}')
            continue

    # print(json.dumps(res, indent=3))

"""The DAG is called 'user_automation' and is set to run once daily. The task is runs the
   python function and perform the fetching and streaming."""
#creating DAG (Directed Acyclic Graph
with DAG('user_automation',
         default_args=default_args,
         schedule='@daily',
         catchup=False) as dag:

    streaming_task = PythonOperator(
        task_id='stream_data_from_api',
        python_callable=stream_data
    )

# stream_data()