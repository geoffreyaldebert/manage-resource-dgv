from pymongo import MongoClient
from datetime import datetime
import json
from dateutil.relativedelta import relativedelta
import boto3
import io
import requests
from botocore.client import Config
import os


def save_to_minio(key, resource):
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_URL'),
        aws_access_key_id=os.getenv('MINIO_USER'),
        aws_secret_access_key=os.getenv('MINIO_PWD'),
        config=Config(signature_version='s3v4')
    )
    response = requests.get(resource['url'])
    s3.upload_fileobj(io.BytesIO(response.content), os.getenv('MINIO_BUCKET'), 'tests-consumer/'+resource['id'])

def manageresource(key, value):
    client = MongoClient('mongodb', 27017)
    db = client['resources']
    col = db.history
    value = json.loads(value)
    if 'resources' in value:
        print('message ok')
        for resource in value['resources']:
            to_insert = {'dataset_id': key, 'resource_id': resource['id'], 'modified_at': datetime.now()}
            print('process ', resource)
            empty = True
            results = col.find({'dataset_id': key, 'resource_id': resource['id']})
            for result in results:
                print('resource already in collection')
                empty = False
                if(result['modified_at'] < datetime.now() + relativedelta(seconds=-10)):
                    # Do something
                    print('can modify')
                    save_to_minio(key, resource)
                    col.replace_one({'dataset_id': key, 'resource_id': resource['id']},to_insert,True)
                else:
                    print('not enough time with last update')
            if empty:
                print('resource not in collection, inserting...')
                save_to_minio(key, resource)
                col.insert_one(to_insert)
    return f'Message processed by celery with key: {key} and value: {value}'