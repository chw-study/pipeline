import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil import parser
import os, re
import requests
from payments import get_numbers, get_mongo_client, get_crosswalk
from tag_training import tag_training_df
from load_workers import get_testers

import logging
logging.basicConfig(level = logging.DEBUG)

class FutureException(Exception):
    pass

def get_windows(start = datetime(2018, 5, 1), weeks = 1):
    times = []
    while start < datetime.utcnow():
        end = start + timedelta(weeks = weeks)
        times.append((start.isoformat(), end.isoformat()))
        start = end
    return times

def make_request(form_id, token, since, until):
    url = 'https://api.typeform.com/forms/{}/responses'.format(form_id)
    q = {'since': since, 'until': until, 'page_size': 1000}
    return requests.get(url,
                       headers = {'Authorization': 'bearer {}'.format(token)},
                       params = q)

def get_responses(form_id):
    token = os.getenv('TYPEFORM_TOKEN')
    responses = []
    windows = get_windows()
    for since,until in windows:
        res = make_request(form_id, token, since, until)
        responses += res.json()['items']
    return responses

def clean_typeform(typeform):
    collapse = lambda df: df.head(1).assign(provided_care = np.any(df.provided_care))

    typeform = typeform[~typeform.provided_care.isna()]
    groups = typeform.groupby(['patientphone', 'patient', 'code', 'visitdate'])
    norms = pd.concat([g for i,g in groups if g.shape[0] == 1])
    funkies = pd.concat([collapse(g) for i,g in groups if g.shape[0] > 1])
    return pd.concat([norms, funkies])


def get_question(response, qid, key):
    return next((a[key] for a in response['answers']
                 if a['field']['id'] == qid), None)

def flatten_response(res):
    d = res['hidden']
    provided_care = get_question(res, 'tN8EHB01kUah', 'boolean')
    if provided_care == None:
        d['called'] = False
    else:
        d['called'] = True
        d['provided_care'] = provided_care
    return d


def get_typeform_responses(form_id):
    responses = get_responses(form_id)
    df = pd.DataFrame([flatten_response(r) for r in responses])
    return df

# COPIED FROM RETRIEVER --> TODO: COMBINE!
def get_service_date(entry):
    report_date = entry['Report_Date']
    date = entry['Service_Date']
    date = re.sub(r'[^\d]+', '.', date)
    date = re.sub(r'^[^\d]', '', date)
    try:
        date = datetime.strptime(date, '%d.%m.%Y')
        if date > report_date:
            raise FutureException('Service date in future: {}'.format(date))
    except Exception as e:
        logging.error(e)
        date = report_date
    return date

def get_attempts(a, idx):
    try:
        return a[idx]
    except:
        return None

def convert_entry(d):
    og = d['originalEntry']
    service_date = get_service_date(og)
    return {
        'attempts': d.get('attempts'),
        'code': d.get('code'), # og??
        'serviceDate': service_date,
        'workerPhone': og['Sender_Phone_Number'],
        'patientPhone': og['Patient_Phone_Number'],
        'patientName': og['Patient_Name'],
    }


def get_og_messages(collection):
    df = pd.DataFrame(list((convert_entry(e) for e in collection.find({}))))
    df['first_attempt'] = df.attempts.map(lambda a: get_attempts(a,0))
    df['last_attempt'] = df.attempts.map(lambda a: get_attempts(a,-1))
    return df


def merge_typeform(messages, typeform):

    typeform = (typeform
                .assign(visitdate = typeform.visitdate.map(parser.parse).map(datetime.date))
                .assign(patient = typeform.patient.str.upper())
                .assign(code = typeform.code.str.upper()))

    messages = (messages.assign(serviceDate = messages.serviceDate.map(datetime.date))
                .assign(patientName = messages.patientName.str.upper())
                .assign(code = messages.code.str.upper()))

    return (messages
            .merge(typeform,
                   how = 'outer',
                   left_on=['patientPhone', 'serviceDate', 'patientName', 'code'],
                   right_on=['patientphone', 'visitdate', 'patient', 'code'],
                   indicator = True)
            # [['_id',
            #   'first_attempt',
            #   'last_attempt',
            #   'called',
            #   '_merge',
            #   'noConsent',
            #   'patientName',
            #   'patientPhone',
            #   'serviceDate',
            #   'training',
            #   'workerPhone',
            #   'code',
            #   'provided_care']]
            )

class DataCorruptionError(BaseException):
    pass



# from pymongo import UpdateOne
# from dotenv import load_dotenv
# load_dotenv()

# client = get_mongo_client()
# messages = get_og_messages(client['healthworkers'].messages)

# form_id = 'a1cQMO'

# typeform = clean_typeform(get_typeform_responses(form_id))

# crosswalk = get_crosswalk('number-changes/number_changes.xlsx')
# numbers = get_numbers('rosters/chw_database_20180608.xlsx')

# testers = get_testers(client['healthworkers'].messages, numbers, crosswalk)
# training_dates = [(n['reporting_number'], n['training_date'])
#                   for n in numbers.to_dict(orient='records')]

# tagged = tag_training_df(training_dates, messages, testers)

# uniques = (tagged
#            .groupby(['patientName', 'code', 'serviceDate', 'patientPhone'])
#            .apply(lambda df: df.head(1)))

# merged = merge_typeform(uniques, typeform)

# missing = merged[(~merged.workerPhone.isin(numbers.reporting_number)) &
#                  (merged.training == False)].shape[0]
# if missing:
#     raise Exception('Missing numbers!!')

# final = (merged
#          .merge(numbers, left_on='workerPhone', right_on='reporting_number')
#          .drop(['_id', 'workerPhone', '_merge'], 1))
