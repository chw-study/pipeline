from lib.utils import *
from datetime import datetime
from dateutil import parser
import pytest

@pytest.fixture()
def message():
     message = {
         'ogServiceDate': '11.2.3',
         'patientName': 'foo',
         'patientPhone': 'foo',
         'code': 'A'
     }
     yield message

@pytest.fixture()
def rawmessage():
    m = {
        "Report_Date" : datetime(2018,2,27,13,8,16),
        "Sender_Phone_Number" : "23276345000",
        "Service_Date" : "10.02.2018",
        "Patient_Name" : "FATMATA bangura",
        "Patient_Phone_Number" : "076345000",
        "Service_Code" : "e"
    }
    yield m

def test_make_id_makes_unique_ids(message):
    a = make_id(message)
    message['patientName'] = 'bar'
    b = make_id(message)
    message['patientPhone'] = 'bar'
    c = make_id(message)
    message['ogServiceDate'] = 'bar'
    d = make_id(message)
    assert(type(a) == str)
    assert(a != b)
    assert(b != c)
    assert(c != d)

def test_convert_parses_valid_service_date(rawmessage):
    c = convert_entry(rawmessage)
    assert(c['serviceDate'] == datetime(2018,2,10))

def test_convert_parses_fallsback_with_future_date(rawmessage):
    rawmessage['Service_Date'] = '29.02.2018'
    c = convert_entry(rawmessage)
    assert(c['serviceDate'] == datetime(2018,2,27))

def test_convert_uppercases_code(rawmessage):
    c = convert_entry(rawmessage)
    assert(c['code'] == 'E')

def test_convert_uppercases_name(rawmessage):
    c = convert_entry(rawmessage)
    assert(c['patientName'] == 'FATMATA BANGURA')

def test_translate_numbers():
    pass
