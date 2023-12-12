from jdatetime import datetime as jdatetime_datetime, timedelta
from persiantools.jdatetime import JalaliDate
import pytz
from pymongo import MongoClient, UpdateMany
import os
import json
import pandas as pd
import cx_Oracle
from mongo_pkg import *
from oracle_pkg import *
import time as t
import asyncio
import logging
import sys

from jdatetime import datetime as jdatetime_datetime, timedelta
from persiantools.jdatetime import JalaliDate

DOC_NAME = 'event'
IP = '10.40.195.156'
SERVICE = 'dw'


#################################
# Insert data into oracle
def test(ds):

    try:
        lst = list(ds)
        values = []

        for row in lst:
            try:

                values.append((
                    str(row["_id"])
                    , str(row["@timestamp"]) if "@timestamp" in row.keys() and row["@timestamp"] is not None else ""
                    , str(row["status"]) if "status" in row.keys() and row["status"] is not None else ""
                       ))
            except Exception as e:
                print('Insert Error :', e)

        max_timestamp = max(row["@timestamp"] for row in lst if "@timestamp" in row)
        max_timestamp = jdatetime_datetime.strptime(max_timestamp, "%Y-%m-%dT%H:%M:%S.%fZ")
        adjusted_max_timestamp = max_timestamp + timedelta(hours=3, minutes=30)
        print(" Max Timestamp:", adjusted_max_timestamp)

        count = len(lst)
        return count
    except Exception as e:
        print('Errorrrr ', e)

def readMongo(min_dop_sequence, max_dop_sequence):
    db = MongoDB()
    collection = db.get_coll(DOC_NAME)
    query = {"dop_sequence": {"$gte": min_dop_sequence, "$lt": max_dop_sequence}}
    cursor = collection.find(query)
    return cursor
#############################################################
#############################################################

latest_sequence = 589479564
print('####' * 15)
chunks = readMongo(latest_sequence-10000, latest_sequence)
count = test(chunks)
print(count)
