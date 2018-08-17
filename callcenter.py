import pandas as pd
import numpy as np
from lib.pipeline import start_pipeline
from lib.utils import get_redis_client
from payments import agg_reports
from bson.json_util import dumps
from datetime import timedelta, datetime
import logging
logging.basicConfig(level = logging.INFO)

def call_counts(messages):
    """ Counts calls per paymentPhone

    :param messages: messages DF without noConsent,
    without training, from last _ weeks.
    """
    return (messages
            .assign(reports = 1)
            .pipe(lambda df: df.assign(called = df.called.astype(int)))
            .groupby('paymentPhone')
            .agg({ 'reports': np.sum, 'called': np.sum }))


def add_needed_calls(counts, target):
    return counts.assign(needed = (counts.reports * target).map(np.ceil) - counts.called)

# do this in such a way as to not call the same person over and over... or maybe you should?
# let's just randomize

def pick_needed_calls(needed, messages):
    lookup = needed.to_dict(orient='index')

    def head(df):
        amt = lookup.get(df.name, {}).get('needed', 0)
        return df.head(int(amt))

    to_call = (messages
               .groupby('paymentPhone')
               .apply(head)
               .reset_index(drop=True))

    # shuffle message order!
    return to_call.sample(frac=1).reset_index(drop=True)

def write_needed_calls(to_call, r):
    pipe = r.pipeline()
    districts = to_call.chw_district.unique().tolist()
    loaded = { d: 0 for d in districts }

    # Replace nan values with None to become null in javascript
    to_call = to_call.where((pd.notnull(to_call)), None)

    for rec in to_call.to_dict(orient='records'):
        district = rec['chw_district']
        d = dumps(rec)
        pipe.lpush(district, d)
        loaded[district] += 1

    for d in districts:
        old_size = r.llen(d)
        new_size = loaded[d]
        logging.info('loading {} new messages in district {}'.format(new_size, d))
        if old_size > new_size:
            pipe.ltrim(d, 0, -loaded[d] - 1)
        else:
            pipe.ltrim(d, 0, loaded[d] - 1)

    pipe.execute()


def add_fake_training_date(df):
    idx = df.training_date.isna()
    df.loc[idx, 'training_date'] = datetime(1970,1,1)
    df.fillna({'workerName' : 'Tester McTesterson'}, inplace=True)
    return df

def ex(thresh, since = timedelta(weeks = 4)):
    r = get_redis_client()
    messages = start_pipeline('')

    training = messages[messages.training == True]

    messages = messages[(messages.serviceDate > datetime.utcnow() - since) &
                        (messages.training == False) &
                        (messages.noConsent == False)]

    # Write messages to be called
    (call_counts(messages)
     .pipe(add_needed_calls, target=thresh)
     .pipe(pick_needed_calls, messages=messages)
     .pipe(write_needed_calls, r=r))

    # Write training messages
    (training
     .assign(chw_district = 'Test')
     .pipe(add_fake_training_date)
     .pipe(write_needed_calls, r=r))

if __name__ == '__main__':
    ex(.2)
