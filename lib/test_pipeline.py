from lib.pipeline import *
from datetime import datetime
import pytest
import pandas as pd
from copy import deepcopy

@pytest.fixture()
def roster():
    df = pd.DataFrame([
        { 'contact_number': 'n', 'phone_ps': 'a', 'reporting_number': 'foo', 'treat': 0, 'training_date': datetime(2018,1,1)},
        { 'contact_number': 'n', 'phone_ps': 'b', 'reporting_number': 'baz', 'treat': 1, 'training_date': datetime(2018,1,1)},
        { 'contact_number': 'n', 'phone_ps': 'c', 'reporting_number': 'bar', 'treat': 2, 'training_date': datetime(2018,1,1)},
        { 'contact_number': 'n', 'phone_ps': 'd', 'reporting_number': 'qux', 'treat': 3, 'training_date': datetime(2018,1,1)},
        { 'contact_number': 'n', 'phone_ps': 'd', 'reporting_number': 'brat', 'treat': 3, 'training_date': datetime(2018,1,1)},
        { 'contact_number': 'n', 'phone_ps': 'f', 'reporting_number': 'ham', 'treat': 3, 'training_date': datetime(2018,1,1)},
    ])

    yield df

@pytest.fixture()
def messages():
    df = pd.DataFrame([
        { 'senderPhone': 'foo', 'otherkey': 'otherval','timestamp': datetime(2018,2,27), 'ogServiceDate': '11.2.2018',  '_id': 'foo'},
        { 'senderPhone': 'bar', 'otherkey': 'otherval','timestamp': datetime(2018,2,27), 'ogServiceDate': '29.2.2018',  '_id': 'bar'},
        { 'senderPhone': 'foo', 'otherkey': 'otherval','timestamp': datetime(2018,2,27), 'ogServiceDate': '1.12.2017',  '_id': 'baz'}
    ])
    df['serviceDate'] = pd.Series([ datetime(2018,1,1,5,35), datetime(2018,2,2,3,45), datetime(2018,2,2,1,10)])
    yield df

def test_translate_numbers_replaces_numbers_in_roster(roster):
    crosswalk = pd.DataFrame([ {'old_number': 'foo', 'new_payment_number': 'newnumber'}])
    new_roster = translate_numbers(roster, crosswalk, old_key = 'reporting_number', new_key = 'reporting_number')
    assert('foo' in roster.reporting_number.tolist())
    assert('newnumber' not in roster.reporting_number.tolist())
    assert('foo' not in new_roster.reporting_number.tolist())
    assert('newnumber' in new_roster.reporting_number.tolist())


def test_translate_numbers_translates_messages_to_new_key(messages):
    crosswalk = pd.DataFrame([ {'old_number': 'foo', 'new_payment_number': 'newnumber'}])
    new_messages = translate_numbers(messages, crosswalk, old_key = 'senderPhone', new_key = 'paymentPhone')
    assert(new_messages.senderPhone.tolist() == messages.senderPhone.tolist())
    assert(new_messages.paymentPhone.tolist() == ['newnumber', 'bar', 'newnumber'])

def test_translate_numbers_with_multiple_levels(messages):
    messages.loc[2,'senderPhone'] = 'foo1'
    crosswalk = pd.DataFrame([
        {'old_number': 'foo', 'new_payment_number': 'newnumber'},
        {'old_number': 'foo1', 'new_payment_number': 'newnumber'}
    ])
    new_messages = translate_numbers(messages, crosswalk, old_key = 'senderPhone', new_key = 'paymentPhone')
    assert(new_messages.senderPhone.tolist() == messages.senderPhone.tolist())
    assert(new_messages.paymentPhone.tolist() == ['newnumber', 'bar', 'newnumber'])

def test_merge_worker_info(messages, roster):
    messages['paymentPhone']= messages.senderPhone
    m = merge_worker_info(messages, roster, ['reporting_number'])
    assert('treat' in m.columns.tolist())
    assert('reporting_number' not in m.columns.tolist())

def test_assign_tester_numbers(messages, roster):
    messages = pd.concat([messages, pd.DataFrame([{ 'senderPhone': 'trainee', 'otherkey': 'otherval' }])], sort=False)
    messages['paymentPhone']= messages.senderPhone
    messages = messages.assign(training = False)
    assign_tester_numbers(messages, roster)
    assert(messages.training.tolist() == [False, False, False, True])

def test_assign_training_messages(messages, roster):
    messages['paymentPhone']= messages.senderPhone

    messages = merge_worker_info(messages, roster, ['reporting_number'])
    messages = messages.assign(training = False)
    assign_training_messages(messages)
    assert(messages.training.tolist() == [True, False, False])

def test_add_db_events(messages):
    events = (
        { 'event': 'called', 'timestamp': datetime(2018,1,3,1,1), 'record': {'_id': 'foo'}, 'updates': { 'code': 'A' }},
        { 'event': 'called', 'timestamp': datetime(2018,1,3,1,1), 'record': {'_id': 'bar'}, 'updates': { 'code': 'B' }})
    new_messages = add_db_events(messages, events)
    assert(new_messages.called.tolist() == [True, True, False])
    assert(new_messages.noConsent.tolist() == [False]*3)
    assert(new_messages.attempted.tolist() == [False]*3)

def test_add_db_events(messages):
    events = (
        { 'event': 'attempted', 'timestamp': datetime(2018,1,3,1,1), 'record': {'_id': 'foo'}},
        { 'event': 'noConsent', 'timestamp': datetime(2018,1,3,1,1), 'record': {'_id': 'bar'}})
    new_messages = add_db_events(messages, events)
    assert(new_messages.called.tolist() == [False]*3)
    assert(new_messages.noConsent.sum() == 1)
    assert(new_messages.attempted.sum() == 1)
    assert(new_messages[new_messages._id == 'foo'].attempted.tolist() == [True])
    assert(new_messages[new_messages._id == 'bar'].attempted.tolist() == [False])

def test_pipeline_fixes_past_and_future_service_dates(messages, roster):
    crosswalk = pd.DataFrame([{'old_number': 'foo', 'new_payment_number': 'newnumber'}])
    events = ()
    transformed = pipeline(messages, events, roster, crosswalk)

    # Not sure why the order is such, but should be deterministic...
    assert(transformed.serviceDate[0] == transformed.timestamp[0])
    assert(transformed.serviceDate[1] == transformed.timestamp[1])
    assert(transformed.serviceDate[2] != transformed.timestamp[2])

def test_pipeline_drops_duplicates(messages, roster):
    m = messages.to_dict(orient='records')
    m = m + [deepcopy(m[1])]
    messages = pd.DataFrame(m)
    crosswalk = pd.DataFrame([ {'old_number': 'foo', 'new_payment_number': 'newnumber'}])
    events = (
        { 'event': 'attempted', 'timestamp': datetime(2018,1,3,1,1), 'record': {'_id': 'foo'}},
        { 'event': 'noConsent', 'timestamp': datetime(2018,1,3,1,1), 'record': {'_id': 'bar'}})
    transformed = pipeline(messages, events, roster, crosswalk)
    assert(messages.shape[0] == 4)
    assert(transformed.shape[0] == 3)
