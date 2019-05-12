from payments import *
from lib.pipeline import pipeline
import pandas as pd
import pytest

records = [
    { 'paymentPhone': '01','timestamp': datetime(2018,3,1),  'serviceDate': datetime(2018,2,1), 'phone_ps': '13',  'treat': 2, 'training_date': datetime(2018,1,1), 'training': False},
    { 'paymentPhone': '01','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,4,1), 'phone_ps': '13',  'treat': 2, 'training_date': datetime(2018,1,1), 'training': False},
    { 'paymentPhone': '02','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,4,1), 'phone_ps': '12', 'treat': 1, 'training_date': datetime(2018,1,1), 'training': False},
    { 'paymentPhone': '03','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,4,1), 'phone_ps': '14', 'treat': 3, 'training_date': datetime(2018,1,1), 'training': False},
    { 'paymentPhone': '04','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,5,1), 'phone_ps': '11', 'treat': 0, 'training_date': datetime(2018,1,1) , 'training': False},
    { 'paymentPhone': '04','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,5,1), 'phone_ps': '11', 'treat': 0, 'training_date': datetime(2018,1,1) , 'training': False},
    { 'paymentPhone': '01','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,5,1), 'phone_ps': '02', 'treat': 1, 'training_date': datetime(2018,1,1), 'training': False},
    { 'paymentPhone': '05','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2100,12,1), 'phone_ps': '01', 'treat': 3, 'training_date': datetime(2018,1,1), 'training': False}, # This one is not paid -- future!
]

fat_records = [
    { 'paymentPhone': '05','timestamp': datetime(2018,m,1),  'serviceDate': datetime(2018,m,3), 'phone_ps': '14', 'treat': 3, 'training_date': datetime(2018,1,1), 'training': False}
for m in range (3,5) for i in range(1,50)]

@pytest.fixture()
def messages():
    yield pd.DataFrame(records)

@pytest.fixture()
def fat_messages():
    yield pd.DataFrame(records + fat_records)

def test_monther_makes_range():
    d = datetime(2018,3,5)
    now = datetime(2018,5,10)
    m = monther(d, now)
    assert(len(m) == 2)
    assert(m[0][0] == datetime(2018, 3, 6))
    assert(m[0][1] == datetime(2018, 4, 6))
    assert(m[1][0] == datetime(2018, 4, 6))
    assert(m[1][1] == datetime(2018, 5, 6))

def test_monther_handles_all_dates():
    now = datetime(2018,5,10)
    d = datetime(2017,1,10)
    assert(len(monther(d, now)) == 15)
    d = datetime(2018,1,9)
    assert(len(monther(d, now)) == 4)
    d = datetime(2017,12,10)
    assert(len(monther(d, now)) == 4)

def test_monther_works_with_now():
    now = datetime(2018,5,10)
    assert(len(monther(now, now)) == 0)

def test_aggs_reports():
    df = pd.DataFrame([
        { 'paymentPhone': 'foo', 'payment_due':datetime(2018,5,1)},
        { 'paymentPhone': 'bar', 'payment_due': datetime(2018,5,2)},
        { 'paymentPhone': 'foo', 'payment_due': datetime(2018,5,1)}])
    agged = agg_reports(df)
    print(agged)
    assert(agged.shape == (2,3))
    assert(agged.reports.tolist() == [1, 2])
    assert(agged.payment_due.tolist() == [datetime(2018,5,2), datetime(2018,5,1)])
    assert(agged.paymentPhone.tolist() == ['bar', 'foo'])

def test_payments(messages):
    workers = calc_payments(messages, pay_workers)
    assert(workers.payment.tolist() == [12000, 12000, 10000, 11000, 10000, 10000])
    assert(workers.reports.tolist() == [1,1,1,1,1,2])

    supers = calc_payments(messages, pay_supers)
    assert(supers.payment.tolist() == [10000, 12000, 10000, 11000, 12000, 10000])


def test_payments_ignores_messages_more_than_6_weeks_old(messages):
    old_message = { 'paymentPhone': '01','timestamp': datetime(2018,5,1),  'serviceDate': datetime(2018,2,1), 'phone_ps': '13',  'treat': 2, 'training_date': datetime(2018,1,1), 'training': False}

    messages = pd.concat([messages, pd.DataFrame(old_message, index=[0])], sort=False)

    workers = calc_payments(messages, pay_workers)
    assert(workers.payment.tolist() == [12000, 12000, 10000, 11000, 10000, 10000])
    assert(workers.reports.tolist() == [1,1,1,1,1,2])
    supers = calc_payments(messages, pay_supers)
    assert(supers.payment.tolist() == [10000, 12000, 10000, 11000, 10000, 12000])


def test_payments_gives_base_for_each_month_with_one_message(messages):
    workers = calc_payments(messages, pay_workers)
    workers[workers.number == '01'].payment.tolist() == [10000, 10000]

def test_payments_caps_at_30(fat_messages):
    workers = calc_payments(fat_messages, pay_workers)
    workers = workers[workers.number == '05']
    assert(workers.payment.tolist() == [40000, 40000])
    supers = calc_payments(fat_messages, pay_supers)
    supers = supers[supers.number == '14']
    assert(supers.payment.tolist() == [41000, 40000])

def test_payment_works_with_promoted_chw(messages):
    pass
