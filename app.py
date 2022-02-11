
from rq import Queue
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import time
from redis import Redis
import tasks
import json
import logging
from kafka import KafkaConsumer
import os
import logging
from pymongo import MongoClient

queue = Queue(connection=Redis(host='localhost',port='6380'))

logging.getLogger().setLevel(logging.INFO)
logging.info('Creating Kafka Consumer')

consumer = KafkaConsumer(
    bootstrap_servers=f'localhost:9092',
    group_id='test',
    reconnect_backoff_max_ms=100000,  # TODO: what value to set here?
)
consumer.subscribe('dataset')
logging.info('Kafka Consumer created')


logging.info('Connect to Mongo')
client = MongoClient('localhost', 27020)
db = client['resources']
col = db.history
logging.info('Mongo connected')


actual_dataset = None

for message in consumer:
    value = message.value
    val_utf8 = value.decode('utf-8').replace('NaN', 'null')
    key = message.key
    data = json.loads(val_utf8)
    logging.info('New message detected, checking dataset {}'.format(key))
    # If message not None = no deletion asked
    if data:
        logging.info(data)
        logging.info('-----------')
        if 'resources' in data:
            for r in data['resources']:
                logging.info('checking resource {}'.format(r['id']))
                date_time_obj = datetime.strptime(r['modified'], '%Y-%m-%d %H:%M:%S.%f')
                # We take into account only resource modified in the last hour
                if(date_time_obj > datetime.now() + relativedelta(hours=-1)):
                    logging.info('No analysis detected recently, continue...')
                    results = col.find({'dataset_id': key, 'resource_id': r['id']})
                    to_insert = {'dataset_id': key, 'resource_id': r['id'], 'modified_at': datetime.now()}
                    # Tricks to resolve the two same following message when resource creation
                    is_result = False
                    for result in results:
                        is_result = True
                        if((date_time_obj < datetime.now() + relativedelta(hours=-1))):
                            col.replace_one({'dataset_id': key, 'resource_id': r['id']},to_insert,True)
                            logging.info('Autorization for further analysis ok...')
                            queue.enqueue(tasks.manage_resource, key.decode('utf-8'), r)

                    if not is_result:
                        col.insert_one(to_insert)
                        logging.info('Autorization for further analysis ok...')
                        queue.enqueue(tasks.manage_resource, key.decode('utf-8'), r)
    else:
        logging.info('Message empty, do not process anything - END')
                        