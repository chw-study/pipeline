from itertools import takewhile, islice, count
import hashlib, logging, re, os
from toolz import get_in
import pandas as pd
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timedelta
from .exceptions import *
import boto3
import redis
import logging
import io


def chunk(n, it):
    src = iter(it)
    return takewhile(bool, (list(islice(src, n)) for _ in count(0)))

def bulk_upsert(coll, records, chunk_size, checked_keys):
    dot = re.compile(r'\.')
    chunked = chunk(chunk_size, records)
    i = 0
    for c in chunked:
        requests = [ UpdateOne({ k: get_in(k.split('.'), obj) for k in checked_keys},
                               { '$setOnInsert': obj },
                               upsert=True) for obj in c ]
        i += len(requests)
        coll.bulk_write(requests, ordered=False)
    return i

def md5(s):
    h = hashlib.new('md5')
    h.update(s.encode('utf-8'))
    return h.hexdigest()

def get_mongo_client(test = False):
    host = os.getenv('MONGO_HOST_TEST') if test else os.getenv('MONGO_HOST')
    client = MongoClient(host,
                     username = os.getenv('MONGO_USER'),
                     password = os.getenv('MONGO_PASSWORD'))
    return client

def get_redis_client():
    host = os.getenv('REDIS_HOST', 'localhost')
    port = int(os.getenv('REDIS_PORT', 6379))
    password = os.getenv('REDIS_PASS', None)
    db = int(os.getenv('REDIS_DB', 0))
    logging.info('Connecting to REDIS: {}:{} DB: {}'.format(host, port, db))
    client = redis.StrictRedis(host=host, password=password, port=port, db=db, decode_responses=True)
    return client

not_d = re.compile(r'[^\d]+')
start = re.compile(r'^[^\d]')

def get_service_date(date, report_date, training_date):
    fallback = report_date.replace(hour=0, minute=0, second=0)
    date = re.sub(not_d, '.', date)
    date = re.sub(start, '', date)
    try:
        date = datetime.strptime(date, '%d.%m.%Y')
        if date > report_date:
            raise MalformedDateException('Service date in future: {}'.format(date))
        if date < training_date :
            raise MalformedDateException('Service date too far in the past: {}'.format(date))
    except MalformedDateException as e:
        logging.debug(e)
        date = fallback
    except ValueError as e:
        logging.debug(e)
        date = fallback
    except Exception as e:
        logging.error(e)
        date = fallback
    return date

def make_id(m):
    return md5('{}{}{}{}'.format(m['ogServiceDate'],
                                  m['patientName'],
                                  m['patientPhone'],
                                  m['code']))

def convert_entry(og):
    """ Changes keys and formats for ID creation """
    e = {
        'ogServiceDate': og['Service_Date'],
        'patientPhone': og['Patient_Phone_Number'],
        'patientName': og['Patient_Name'].upper(),
        'senderPhone': og['Sender_Phone_Number'],
        'timestamp': og['Report_Date'],
        'code': og['Service_Code'].upper()
    }
    e['_id'] = make_id(e)
    return e

def get_messages_df(collection):
    df = pd.DataFrame(list((convert_entry(e) for e in collection.find({}))))
    return df

def get_events(collection):
    return collection.find({})

def get_roster(path):
    df = pd.read_excel(path)
    df = df.rename(columns = {'chw_name': 'name'})
    df['reporting_number'] = df['z08_2'].astype(str)
    df['training_date'] = df.training_date.map(lambda d: datetime.strptime(d, '%d.%m.%y'))
    return df

def get_latest_s3(bucket, prefix):
    s3 = boto3.client('s3')
    objects = s3.list_objects(Bucket=bucket, Prefix=prefix)
    s = sorted(objects['Contents'],
               key=lambda o: o['LastModified'])
    key = s[-1]['Key']
    return 's3://{}/{}'.format(bucket, key)

def get_crosswalk():
    path = get_latest_s3('healthworkers-payments', 'number_changes/')
    crosswalk = pd.read_excel(path)
    crosswalk['old_number'] = crosswalk.old_number.astype(str)
    crosswalk['new_payment_number'] = crosswalk.last_number.astype(str)
    return crosswalk

def write_to_s3(df, bucket, key):
    out = io.StringIO()
    size = df.shape[0]
    df.to_csv(out, index=False)
    logging.info('REPORTS: Writing {} records to :{}/{}'.format(size, bucket, key))
    s3 = boto3.client('s3')
    s3.put_object(
        Bucket=bucket,
        Key="{1}-{0:%Y-%m-%d_%H:%M}.csv".format(datetime.now(), key),
        Body=out.getvalue()
    )
