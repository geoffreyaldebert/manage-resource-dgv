from datetime import datetime, timedelta
import time
from redis import Redis
from rq import Queue
import tasks
import json
import logging
from kafka import KafkaConsumer
import os

queue = Queue(connection=Redis(host='localhost',port='6380'))

logging.info('Creating Kafka Consumer')
consumer = KafkaConsumer(
    bootstrap_servers=f'localhost:9092',
    group_id='test',
    reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
)
consumer.subscribe('dataset')
logging.info('Kafka Consumer created')

for message in consumer:
    value = message.value
    val_utf8 = value.decode('utf-8').replace('NaN', 'null')
    key = message.key
    index = message.topic
    print(key, val_utf8)
    logging.info(f'Message recieved with key: {key} and value: {value}')
    queue.enqueue(tasks.manageresource, key.decode('utf-8'), val_utf8)
