from pymongo import MongoClient
import boto3
from botocore.client import Config
import io
import requests
import os
from csv_detective.explore_csv import routine
from dotenv import load_dotenv
import logging
import string
import csv
import pandas as pd

load_dotenv()

def is_csv(infile):
    try:
        with open(infile, newline='') as csvfile:
            start = csvfile.read(4096)
            # isprintable does not allow newlines, printable does not allow umlauts...
            if not all([c in string.printable or c.isprintable() for c in start]):
                return False
            dialect = csv.Sniffer().sniff(start)
            return True
    except csv.Error:
        # Could not get a csv dialect -> probably not a csv.
        return False
    except:
        # Unknown format, need to do further analysis ?
        return False

def process_csv_detective_report(result):
    report = {}
    report['csv-analysis:file_type'] = 'csv'
    report['csv-analysis:encoding'] = result['encoding']
    report['csv-analysis:separator'] = result['separator']
    report['csv-analysis:header_row_idx'] = result['header_row_idx']
    report['csv-analysis:headers'] = result['header']
    report['csv-analysis:total_lines'] = result['total_lines']
    
    columns_types_details = []
    columns_types = []
    for column in result['columns']:
        column_report = {}
        column_report['column_type'] = column
        column_report['columns'] = []
        for check in result['columns'][column]:
            if(check['score_rb'] > 0.75):
                column_report['columns'].append(check['colonne'])
        if(len(column_report['columns']) != 0):
            columns_types_details.append(column_report)
            columns_types.append(column)

    report['csv-analysis:columns_types_details'] = columns_types_details
    report['csv-analysis:columns_types'] = columns_types

    return report

def save_resource_to_minio(key, resource):
    logging.info('Saving to minio')
    s3 = boto3.client(
        's3',
        endpoint_url=os.getenv('MINIO_URL'),
        aws_access_key_id=os.getenv('MINIO_USER'),
        aws_secret_access_key=os.getenv('MINIO_PWD'),
        config=Config(signature_version='s3v4')
    )
    response = requests.get(resource['url'])
    s3.upload_fileobj(io.BytesIO(response.content), os.getenv('MINIO_BUCKET'), 'tests-consumer/'+key+'/'+resource['id'])
    logging.info('Resource saved into minio at {}'.format(os.getenv('MINIO_URL')+os.getenv('MINIO_BUCKET')+'/tests-consumer/'+key+'/'+resource['id']))

def update_extras_dgv(dataset_id, resource, report):
    extras = {**resource['extras'], **report}
    HEADERS = {
        'X-API-KEY': os.getenv('API_KEY'),
    }
    obj = { 'extras': extras }
    url = os.getenv('API_URL') + 'datasets/{}/resources/{}/'.format(dataset_id, resource['id'])
    response = requests.put(url, json=obj, headers=HEADERS)
    if response.status_code != 200 :
        logging.info('Report inserted as extras in data.gouv.fr')

def check_geo(key, resource, report):
    if(('code_commune_insee' in report['csv-analysis:columns_types']) & ('commune' not in  report['csv-analysis:columns_types'])):
        logging.info('Retrieving communes...')
        df = pd.read_csv(os.getenv('MINIO_URL')+os.getenv('MINIO_BUCKET')+'/tests-consumer/'+key+'/'+resource['id'],sep=report['csv-analysis:separator'],header=report['csv-analysis:header_row_idx'],dtype=str)
        code_coms = []
        list_coms = []
        for item in report["csv-analysis:columns_types_details"]:
            if item['column_type'] == 'code_commune_insee':
                for col in item['columns']:
                    code_coms = code_coms + list(df[col].unique())
        
        for cc in code_coms:
            r = requests.get(os.getenv('API_GEO')+str(cc))
            data = r.json()
            if len(data) > 0:
                com = data[0]['nom']
                if(com not in list_coms):
                    list_coms.append(com)

        report['csv-analysis:commune'] = list_coms
        report['csv-analysis:code_commune'] = code_coms
        logging.info('communes and code_communes retrieved!')
        return report
    else:
        # do something if commune is in types
        return report

def check_siren(key, resource, report):
    if('siren' in report['csv-analysis:columns_types']):
        logging.info('Retrieving entreprises...')
        df = pd.read_csv(os.getenv('MINIO_URL')+os.getenv('MINIO_BUCKET')+'/tests-consumer/'+key+'/'+resource['id'],sep=report['csv-analysis:separator'],header=report['csv-analysis:header_row_idx'],dtype=str)
        code_siren = []
        list_siren = []
        for item in report["csv-analysis:columns_types_details"]:
            if item['column_type'] == 'siren':
                for col in item['columns']:
                    code_siren = code_siren + list(df[col].unique())
        
        for cs in code_siren:
            r = requests.get(os.getenv('API_SIRENE')+str(cs))
            data = r.json()
            if len(data[0]['unite_legale']) > 0:
                siren = data[0]['unite_legale'][0]['nom_complet']
                if(siren not in list_siren):
                    list_siren.append(siren)

        report['csv-analysis:siren_nom_complet'] = list_siren
        report['csv-analysis:siren'] = code_siren
        logging.info('siren and name of societies retrieved!')
        return report
    else:
        # do something if commune is in types
        return report

def enrich_report(key, resource, report):
    report = check_geo(key, resource, report)
    report = check_siren(key, resource, report)
    return report

def analyze_csv_detective(key, resource):
    logging.info('Analyze via csv-detective')
    url = os.getenv('MINIO_URL')+os.getenv('MINIO_BUCKET')+'/tests-consumer/'+key+'/'+resource['id']
    r = requests.get(url, allow_redirects=True)
    open('/tmp/'+resource['id'], 'wb').write(r.content)
    isCsv = is_csv('/tmp/'+resource['id'])
    if not isCsv:
        logging.info('Probably not a csv, end of analysis')
        return
    logging.info('Seems to be valid csv, continue...')
    results = routine('/tmp/'+resource['id'], output_mode='ALL')
    os.remove('/tmp/'+resource['id'])
    report = process_csv_detective_report(results)
    report = enrich_report(key, resource, report)
    update_extras_dgv(key, resource, report)

def manage_resource(key, resource):
    logging.getLogger().setLevel(logging.INFO)
    logging.info('Processing task for resource {} in dataset {}'.format(resource['id'],key))
    save_resource_to_minio(key, resource)
    analyze_csv_detective(key, resource)
    return 'Resource processed {} - END'.format(resource['id'])