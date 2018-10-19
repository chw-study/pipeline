from lib.utils import get_roster, get_events, get_crosswalk, get_mongo_client, get_messages_df, get_service_date
import logging
import pandas as pd


def tag_training_messages(messages, numbers, crosswalk):
    """ Tags messages as training based on dates/numbers """
    pass

def translate_numbers(df, crosswalk, old_key, new_key):
    d = df.merge(crosswalk, how = 'left', left_on = old_key, right_on= 'old_number')
    idx_old = d.new_payment_number.isna()
    idx_new = d.new_payment_number.notna()
    d.loc[idx_old, new_key] = d[idx_old][old_key]
    d.loc[idx_new, new_key] = d[idx_new].new_payment_number
    return d.drop(crosswalk.columns, 1)

def merge_worker_info(messages, roster, drop_keys):
    logging.debug('PIPELNE: Merging worker info')
    m = messages.merge(roster, how = 'left', left_on = 'paymentPhone', right_on = 'reporting_number')
    return m.drop(drop_keys, 1).rename(columns = { 'name': 'workerName' })

def assign_tester_numbers(messages, roster):
    logging.debug('PIPELNE: Tagging tester messages.')
    idx = ~messages.paymentPhone.isin(roster.reporting_number)
    messages.loc[idx, 'training'] = True
    return messages

def assign_training_messages(messages):
    logging.debug('PIPELNE: Tagging training messages.')
    message_days = messages.serviceDate.map(lambda d: d.replace(hour=0,minute=0,second=0))
    idx = messages.training_date >= message_days
    messages.loc[idx, 'training'] = True
    return messages

def add_db_events(messages, events):
    logging.debug('PIPELNE: Adding events from DB')
    messages['called'] = False
    messages['noConsent'] = False
    messages['attempted'] = False
    d = messages.set_index('_id').to_dict(orient='index')
    for e in events:
        key = e['event']
        i = e['record']['_id']
        try:
            d[i][key] = True
            logging.debug('SUCCESS')
        except KeyError:
            logging.debug('KEY ERROR: {}'.format(i))
            pass
    return (pd.DataFrame
            .from_dict(d, orient='index')
            .reset_index()
            .rename(columns = {'index': '_id'}))

def add_service_date(messages):
    fn = lambda r: get_service_date(r['ogServiceDate'],
                                    r['timestamp'],
                                    r['training_date'])
    service_date = messages.apply(fn, axis=1)
    return messages.assign(serviceDate = service_date)

def pipeline(messages, events, roster, crosswalk):
    k = 'reporting_number'
    roster = translate_numbers(roster, crosswalk, old_key = k, new_key = k)
    return (messages
            # Remove duplicates based on above id
            .drop_duplicates('_id')
            # Convert numbers to current payment number
            .pipe(translate_numbers,
                  crosswalk = crosswalk,
                  old_key = 'senderPhone',
                  new_key = 'paymentPhone')
            # tag name/district/area/training-day of worker
            .pipe(merge_worker_info,
                  roster = roster,
                  drop_keys = ['reporting_number', 'contact_number'])
            # add serviceDate
            .pipe(add_service_date)
            # Add column for training
            .assign(training = False)
            # Tag training messages from non-worker numbers
            .pipe(assign_tester_numbers, roster=roster)
            # Tag training messages from training day of worker
            .pipe(assign_training_messages)
            # Add events: no-consent, attempts, and called
            .pipe(add_db_events, events = events))


def start_pipeline(pre = '../'):
    roster = get_roster(pre+'rosters/chw.xlsx')
    crosswalk = get_crosswalk()
    client = get_mongo_client()
    events = get_events(client['healthworkers'].events)
    rawmessages = get_messages_df(client['healthworkers'].rawmessages)
    messages = pipeline(rawmessages, events, roster, crosswalk)
    return messages
