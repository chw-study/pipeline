from callcenter import *
from datetime import datetime
import pandas as pd
import pytest
import redis
import json
from time import sleep
from bson.json_util import dumps


records = [
    { 'paymentPhone': 'bar', 'serviceDate': datetime(2018,4,1), 'called': True, 'noConsent': False, 'attempted': False},
    { 'paymentPhone': 'bar', 'serviceDate': datetime(2018,4,2), 'called': False, 'noConsent': False, 'attempted': False},
    { 'paymentPhone': 'foo', 'serviceDate': datetime(2018,5,1), 'called': False, 'noConsent': False, 'attempted': False},
]

foo_records = [
    { 'paymentPhone': 'bar', 'serviceDate': datetime(2018,4,1), 'chw_district': 'foo', 'attempted': False },
    { 'paymentPhone': 'bar', 'serviceDate': datetime(2018,5,1), 'chw_district': 'foo', 'attempted': False }
]

baz_records = [
    { 'paymentPhone': 'bar', 'serviceDate': datetime(2018,5,1), 'chw_district': 'baz', 'attempted': False }
]

@pytest.fixture()
def to_write():
    yield pd.DataFrame(foo_records + baz_records)

@pytest.fixture()
def messages():
    yield pd.DataFrame(records)

@pytest.fixture(scope='module')
def r():
    r = redis.StrictRedis(host='localhost', port=6379, db=0)
    yield r
    r.flushdb()

def test_call_counts(messages):
    df = call_counts(messages)
    assert(df.reports.tolist() == [2,1])
    assert(df.called.tolist() == [1,0])

def test_call_counts_removes_noConsent(messages):
    df = call_counts(messages)

def test_needed_calls(messages):
    counts = call_counts(messages)
    df = add_needed_calls(counts, .6)
    assert(df.needed.tolist() == [1,1])

def test_pick_needed_calls(messages):
    counts = call_counts(messages)
    df = add_needed_calls(counts, .6)
    needed = pick_needed_calls(df, messages)
    assert(needed.shape[0] == 2)
    assert(needed.called.tolist() == [False, False])

def test_write_needed_calls_leaves_only_new_records(r, to_write):
    r.lpush('foo', 'bar')
    r.lpush('baz', 'bar')
    write_needed_calls(to_write, r)
    assert(json.loads(r.rpop('foo')) == json.loads(dumps(foo_records[0])))
    assert(json.loads(r.rpop('foo')) == json.loads(dumps(foo_records[1])))
    assert(r.rpop('foo') == None)
    assert(json.loads(r.rpop('baz')) == json.loads(dumps(baz_records[0])))
    assert(r.rpop('baz') == None)


def test_write_needed_calls_works_with_None_district(r):
    to_write = pd.DataFrame([{'chw_district': 'foo'}, {'chw_district': None}])
    write_needed_calls(to_write, r)
    assert(r.rpop('foo') != None)

def test_write_needed_calls_works_with_NaNs(r):
    to_write = pd.DataFrame([{'chw_district': 'foo', 'bar': np.nan}])
    write_needed_calls(to_write, r)
    # Is this really the behavior we want???
    assert(r.rpop('foo') == b'{"bar": null, "chw_district": "foo"}')

def test_write_needed_calls_works_with_large_curr_queue(r, to_write):
    # removes everything from old queue
    r.lpush('foo', '1')
    r.lpush('foo', '2')
    r.lpush('foo', '3')
    foo = {'chw_district': 'foo'}
    write_needed_calls(pd.DataFrame([foo]), r)
    assert(json.loads(r.rpop('foo')) == foo)
    assert(r.rpop('foo') == None)
