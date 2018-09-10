import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime, timedelta
from dateutil import parser
import os, re
import requests
from lib.utils import get_roster, get_mongo_client, get_crosswalk, write_to_s3
from lib.pipeline import start_pipeline
from pymongo import UpdateOne
from dotenv import load_dotenv
import logging
logging.basicConfig(level = logging.INFO)


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
    typeform = pd.concat([norms, funkies])
    return (typeform
            .assign(visitdate = (typeform.visitdate
                                 .map(parser.parse)
                                 .map(datetime.date)
                                 .map(str)))
            .assign(patient = typeform.patient.str.upper())
            .assign(code = typeform.code.str.upper()))

def get_question(response, qid, key):
    if response['answers']:
        return next((a[key] for a in response['answers']
                     if a['field']['id'] == qid), None)
    else:
        logging.error('No answer in the following response: {}'.format(response))

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

def _merger(messages, typeform, date):
    # TODO: replace this with the ID when we have v2 running.

    lefts = ['senderPhone', 'patientPhone', 'patientName', 'code']
    rights = ['workerphone', 'patientphone', 'patient', 'code']
    if date:
        lefts += ['serviceDate']
        rights += ['visitdate']

    return (messages
            .merge(typeform,
                   how = 'outer',
                   left_on=lefts,
                   right_on=rights,
                   indicator = True,
                   suffixes = ('_app', '_typeform')))


def merge_typeform(messages, typeform):
    typeform = typeform.assign(visitdate = typeform.visitdate.astype(str))
    messages = messages.assign(serviceDate = messages.serviceDate.astype(str))

    first = _merger(messages, typeform, True)
    orphans = first[first._merge == 'right_only']
    orphans = orphans.assign(called = orphans.called_typeform)[typeform.columns]
    first = first[first._merge != 'right_only']
    second = _merger(messages, orphans, False)
    orphans = second[second._merge == 'right_only']
    second = second[second._merge == 'both']

    merged = (pd.concat([first, second])
              .drop(['_id',
                   'messageid',
                   'patient',
                   'patientphone',
                   'worker',
                   'workerphone'], 1))

    return merged, orphans

class DataCorruptionError(BaseException):
    pass

if __name__ == '__main__':
    load_dotenv()
    form_id = 'a1cQMO'
    typeform = clean_typeform(get_typeform_responses(form_id))
    messages = start_pipeline('')
    merged,_ = merge_typeform(messages, typeform)
    merged = merged.drop(['_merge'], 1)
    write_to_s3(merged, 'healthworkers-exports', 'reports-v2/report')
