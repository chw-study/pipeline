import pandas as pd
from pymongo import MongoClient, InsertOne
import numpy as np
import os, re
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta as rd
from functools import reduce
from itertools import islice, takewhile, count
from lib.exceptions import *
from lib.pipeline import *
from lib.utils import *

import boto3
import io

import logging

def monther(date, now = datetime.utcnow()):
    date = date + timedelta(days = 1)
    months = rd(now, date).months
    return [(date + rd(months = m), date + rd(months = m+1))
            for m in range(months)]

def _payment_date(df):
    """ Adds payment date given DF with uniform training date """
    training_date = df.training_date.values[0]
    training_date = pd.Timestamp(training_date).to_pydatetime()
    for start,end in monther(training_date):
        idx = (df.serviceDate >= start) & (df.serviceDate < end)
        df.loc[idx, 'payment_due'] = end
    return df

def add_payment_date(messages):
    return messages.groupby('training_date').apply(_payment_date)

def agg_reports(df):
    others = df.groupby(['paymentPhone', 'payment_due']).agg('first')
    reports = (df
               .assign(reports=1)
               .groupby(['paymentPhone', 'payment_due'])
               .agg({ 'reports': np.sum }).reports)
    return (others.assign(reports = reports)
            .reset_index())

def get_count_df(messages):
    drop_keys = ['_id', 'code', 'ogServiceDate', 'serviceDate',
                 'patientName', 'patientPhone', 'timestamp',
                 'training', 'called', 'noConsent', 'attempted']
    messages = messages[messages.training == False]
    return (messages
            .pipe(add_payment_date)
            .pipe(agg_reports)
            .drop(drop_keys, errors = 'ignore')
            .pipe(lambda df: df.assign(counted = np.minimum(df.reports, 30))))

def bonus_worker(treat, counted, **kwargs):
    if treat == 2:
        return counted*2000
    if treat == 3:
        return counted*1000
    return 0

def bonus_super(treat, counted, **kwargs):
    if treat == 1:
        return counted*2000
    if treat == 3:
        return counted*1000
    return 0

def base(df):
    df = df[df.counted > 0] # should be unnecesary
    return df.assign(base = 10000)

def bonus(df, fn):
    bonuses = [fn(**d) for d in df.to_dict(orient='records')]
    return df.assign(bonus = bonuses)

def pay_supers(df):
    return (df
            .groupby(['phone_ps', 'payment_due'])
            .agg({ 'counted': np.sum, 'treat': 'first', 'reports': np.sum})
            .reset_index()
            .pipe(base)
            .pipe(bonus, fn = bonus_super)
            .pipe(lambda df: df.assign(payment = df.bonus + df.base))
            .rename( columns = {'phone_ps': 'number'})
            .pipe(lambda df: df.assign(number = df.number.astype(int).astype(str)))
            [['number', 'payment', 'reports', 'payment_due']])

def calc_payment_per_worker(df):
    return (df
            .pipe(base)
            .pipe(bonus, fn = bonus_worker)
            .pipe(lambda df: df.assign(payment = df.bonus + df.base)))

def pay_workers(df):
    return (calc_payment_per_worker(df)
            .rename( columns = {'paymentPhone': 'number'})
            [['number', 'payment', 'reports', 'payment_due']])

def calc_payments(messages, fn):
    return (get_count_df(messages)
            .pipe(fn)
            .sort_values('payment_due'))

def calcs():
    messages = start_pipeline('')
    workers = calc_payments(messages, pay_workers)
    supers = calc_payments(messages, pay_supers)
    return workers, supers

if __name__ == '__main__':
    workers, supers = calcs()
    write_to_s3(workers, 'healthworkers-payments', 'v2/workers')
    write_to_s3(supers, 'healthworkers-payments', 'v2/supers')
