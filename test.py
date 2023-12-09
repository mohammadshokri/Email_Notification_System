ubuntu@datapipeline:/python-service$ cat ETL-event.py
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

# logging.basicConfig(filename='logs/event-etl.log', level=logging.INFO)
log_path = 'etl-event.log'  # Specify the log file path inside the container

logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

DOC_NAME = 'event'
IP = '10.40.195.156'
SERVICE = 'dw'


#################################
# Insert data into oracle
def insertOracle(ds, conn):
    print('inserting in db...')
    with conn.cursor() as cursor:
        try:
            lst = list(ds)
            change_key = 'exception'
            for sub in lst:
                if change_key in sub and sub[change_key] is not None:
                    exception_str = sub[change_key]
                    # Replace multiple patterns in a single pass using a loop
                    for replace_from, replace_to in [('[', '{"'), (']', '"}'), ('=', '  :  '),
                                                     ('\n', ','), ('{,', '{"'), (',}', '"}'),
                                                     ('{",', '{"'), (',"}', '"}'), ('  :  ', '"  :  "'),
                                                     (',', '","'), ('"{', '{"'), ('}"', '"}')]:
                        exception_str = exception_str.replace(replace_from, replace_to)

                    try:
                        my_dict = eval(exception_str)
                        my_dict = {key.replace(' ', ''): value for key, value in my_dict.items()}
                        sub['exception_dict'] = my_dict
                        sub[change_key] = exception_str.replace(' ', '')
                    except:
                        continue

            values = []

            for row in lst:
                try:
                    values.append((
                        str(row["_id"])
                        ,str(row["response"][:32000]) if "response" in row.keys() and row["response"] is not None else ""
                    , str(row["@timestamp"]) if "@timestamp" in row.keys() and row["@timestamp"] is not None else ""
                        , str(row["status"]) if "status" in row.keys() and row["status"] is not None else ""
                        , str(row["endDateTime"]) if "endDateTime" in row.keys() and row[
                            "endDateTime"] is not None else ""
                        , str(row["session"]) if "session" in row.keys() and row["session"] is not None else ""
                        , str(row["message"]) if "message" in row.keys() and row["message"] is not None else ""
                        , str(row["exception"]) if "exception" in row.keys() and row["exception"] is not None else ""
                        , str(row["name"]) if "name" in row.keys() and row["name"] is not None else ""
                        , str(row["startDateTime"]) if "startDateTime" in row.keys() and row[
                            "startDateTime"] is not None else ""
                        , str(row["request"]) if "request" in row.keys() and row["request"] is not None else ""
                        , str(row["@version"]) if "@version" in row.keys() and row["@version"] is not None else ""
                        , str(row["session"].get("businessDomain")) if "session" in row.keys() and row[
                            "session"] is not None else ""  # row["session"].get("businessDomain") is not None else ""
                        , str(row["session"].get("clientCorrelationID")) if "session" in row.keys() and row[
                            "session"] is not None else ""
                        # row["session"].get("clientCorrelationID") is not None else ""
                        , str(row["session"].get("clientID")) if "session" in row.keys() and row[
                            "session"] is not None else ""  # row["session"].get("clientID") is not None else ""
                        , str(row["session"].get("communicationChannel")) if "session" in row.keys() and row[
                            "session"] is not None else ""
                        # row["session"].get("communicationChannel") is not None else ""
                        , str(row["session"].get("correlationID")) if "session" in row.keys() and row[
                            "session"] is not None else ""  # row["session"].get("correlationID") is not None else ""
                        , str(row["session"].get("requestID")) if "session" in row.keys() and row[
                            "session"] is not None else ""  # row["session"].get("requestID") is not None else ""
                        , str(row["session"].get("trackerID")) if "session" in row.keys() and row[
                            "session"] is not None else ""  # row["session"].get("trackerID") is not None else ""
                        , str(row["spanId"]) if "spanId" in row.keys() and row["spanId"] is not None else ""
                        , str(row["tags"]) if "tags" in row.keys() and row["tags"] is not None else ""
                        , str(row["version"]) if "version" in row.keys() and row["version"] is not None else ""
                        , str(row["businessSpanId"]) if "businessSpanId" in row.keys() and row[
                            "businessSpanId"] is not None else ""
                        , str(row["eventType"]) if "eventType" in row.keys() and row["eventType"] is not None else ""
                        , str(row["duration"]) if "duration" in row.keys() and row["duration"] is not None else ""
                        , str(row["traceId"]) if "traceId" in row.keys() and row["traceId"] is not None else ""
                        , str(row["appName"]) if "appName" in row.keys() and row["appName"] is not None else ""
                        , str(row["method"]) if "method" in row.keys() and row["method"] is not None else ""
                        , str(row["statusCode"]) if "statusCode" in row.keys() and row["statusCode"] is not None else ""
                        , str(row["path"]) if "path" in row.keys() and row["path"] is not None else ""
                        , str(row["nginxRequestId"]) if "nginxRequestId" in row.keys() and row[
                            "nginxRequestId"] is not None else ""
                        , str(row["ip"]) if "ip" in row.keys() and row["ip"] is not None else ""
                        , str(row["resultCode"]) if "resultCode" in row.keys() and row["resultCode"] is not None else ""
                        , str(row["exceptio"]) if "exceptio" in row.keys() and row["exceptio"] is not None else ""
                        , str(row["exceptionCode"]) if "exceptionCode" in row.keys() and row[
                            "exceptionCode"] is not None else
                        str(row["exception_dict"].get("exceptionCode")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("exceptionCode") is not None else ""
                        , str(row["exceptionMessage"]) if "exceptionMessage" in row.keys() and row[
                            "exceptionMessage"] is not None else
                        str(row["exception_dict"].get("exceptionMessage")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("exceptionMessage") is not None else "None"
                        , str(row["exceptionMetadata"]) if "exceptionMetadata" in row.keys() and row[
                            "exceptionMetadata"] is not None else
                        str(row["exception_dict"].get("metadata")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("metadata") is not None else "None"
                        , str(row["exceptionKey"]) if "exceptionKey" in row.keys() and row[
                            "exceptionKey"] is not None else
                        str(row["exception_dict"].get("exceptionKey")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("exceptionKey") is not None else ""
                        , str(row["exceptionType"]) if "exceptionType" in row.keys() and row[
                            "exceptionType"] is not None else
                        str(row["exception_dict"].get("exceptionType")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("exceptionType") is not None else ""
                        ,
                        str(row["exception_dict"].get("exceptionCodeValue")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("exceptionCodeValue") is not None else ""
                        , str(row["exception_dict"].get("httpStatus")) if "exception_dict" in row.keys() and row[
                            "exception_dict"].get("httpStatus") is not None else ""
                        , str(row["dop_sequence"]) if "dop_sequence" in row.keys() and row[
                            "dop_sequence"] is not None else ""
                           ))
                except Exception as e:
                    print('Insert Error :', e)
                    logging.info(e)
            try:
                cursor.executemany("INSERT /*+ append */ INTO event_prim(ID_,RESPONSE,TIMESTAMP_,STATUS,ENDDATETIME,SESSION_,\
                                    MESSAGE,EXCEPTION_,NAME,STARTDATETIME,REQUEST,VERSION_,SESSION_BUSINESSDOMAIN,\
                                    SESSION_CLIENTCORRELATIONID,SESSION_CLIENTID,SESSION_COMMUNICATIONCHANNEL,SESSION_CORRELATIONID,\
                                    SESSION_REQUESTID,SESSION_TRACKERID,SPANID,TAGS,VERSION,BUSINESSSPANID,EVENTTYPE,DURATION,TRACEID,\
                                    APPNAME,METHOD,STATUSCODE,PATH_,NGINXREQUESTID,IP,RESULTCODE,EXCEPTIO,EXCEPTIONCODE,\
                                    EXCEPTIONMESSAGE,EXCEPTIONMETADATA,EXCEPTIONKEY,EXCEPTIONTYPE,EXCEPTION_CODEVALUE,EXCEPTION_HTTPSTATUS, DOP_SEQUENCE ) \
                                    values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,:19,:20,:21,:22,:23,:24,:25,:26,\
                                    :27,:28,:29,:30,:31,:32,:33,:34,:35,:36,:37,:38,:39,:40,:41,:42)", values)
            except Exception as e:
                print('Insert Error :', e)
                logging.info(e)
            print('affter')
            conn.commit()
            count = len(lst)
            return count
        except Exception as e:
            print('Errorrrr ', e)
            logging.info(e)


def readMongo(min_dop_sequence, max_dop_sequence):
    db = MongoDB()
    collection = db.get_coll(DOC_NAME)
    query = {"dop_sequence": {"$gte": min_dop_sequence, "$lt": max_dop_sequence}}
    cursor = collection.find(query)
    return cursor
#############################################################
#############################################################
async def transferDataDB(conn):
    try:
        latest_sequence = db.getLatestSequence()+1
        #latest_sequence = 589479564

    except Exception as e:
        print('Fist____Errorrrr ', e)
        logging.info(e)
    chunk_size = 10000
    while True:
        try:
            t1 = t.time()
            min_dop_sequence = latest_sequence
            max_dop_sequence= min_dop_sequence + chunk_size
            print('####' * 15)
            chunks = readMongo(min_dop_sequence, max_dop_sequence)
            count = insertOracle(chunks, conn)
            latest_sequence = min_dop_sequence + count
            print(min_dop_sequence)
            print(max_dop_sequence)
            print(count)
            print(t.time() - t1)
            await asyncio.sleep(1)
        except Exception as e:
            print('Error in the while segment of your code! ', e)
            logging.info(e)


db = OraDB(IP, SERVICE)
conn = db.oraConnect()

print("Starting ETL Service")

loop = asyncio.get_event_loop()
try:
    asyncio.ensure_future(transferDataDB(conn))
    loop.run_forever()
except KeyboardInterrupt:
    pass
finally:
    print("Closing ETL Service")
    loop.close()
    conn.close()
