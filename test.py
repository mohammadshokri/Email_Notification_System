from pymongo import MongoClient, UpdateMany
import cx_Oracle
import os
import json
import pandas as pd
import ast
import re
import bson
import time as t
import asyncio
import logging
import numpy as np
from mongo_pkg import *
from oracle_pkg import *
import sys

log_path = '/app/logs/etl-exception-docker.log'  # Specify the log file path inside the container
logging.basicConfig(filename=log_path, level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
DOC_NAME = 'event'
IP = '10.40.195.156'
SERVICE = 'dw'


def getExceptMeta(conn):
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                "select ID_,TIMESTAMP_,EXCEPTIONMETADATA,ID,SESSION_REQUESTID from event_prim where except_flag is null")
            f = cursor.fetchall()
            return f


        except Exception as e:
            print(e)
            logging.info(e)
def updateOneExceptFlag(flag, p_id, conn):
    with conn.cursor() as cursor:
        try:
            cursor.execute(
                "update galaxy_ai.event_prim set except_flag=:1 where id=:2"
                , [flag, int(p_id)])

        except Exception as e:
            print(e)
            logging.info(e)

        conn.commit()
def updateManyExceptFlag(l_p_id):
    bind_names = ",".join(":" + str(i + 1) for i in range(len(l_p_id)))

    with conn.cursor() as cursor:
        try:
            cursor.execute(
                """update galaxy_ai.event_prim set except_flag=1 where id in ({}) """.format(bind_names, tuple(l_p_id)),
                l_p_id)

            conn.commit()
            print("success update")


        except Exception as e:
            print(e)
            logging.info(e)
def insertOracle_Except_Meta(df_except, conn):
    list_id = []
    with conn.cursor() as cursor:
        try:
            for index, row in df_except.iterrows():
                try:
                    cursor.execute("insert into event_except_meta(id_,timestamp_,event_prim_id,\
                                   session_requestid,exceptionmetadata_systemname,\
                                   exceptionmetadata_mahtaberrordata_transactiontype,\
                                   exceptionmetadata_mahtaberrordata_tracenumber,\
                                   exceptionmetadata_mahtaberrordata_referencenumber,\
                                   exceptionmetadata_mahtaberrordata_requestuuid,\
                                   exceptionmetadata_mahtaberrordata_requestdate,\
                                   exceptionmetadata_mahtaberrordata_errorcode,\
                                   exceptionmetadata_mahtaberrordata_mahtabdescription,\
                                   exceptionmetadata_checksource,exceptionmetadata_accountnumber,\
                                   exceptionmetadata_validationtype,exceptionmetadata_problem,\
                                   exceptionmetadata_fieldname,\
                                   exceptionmetadata_channelproxyerrordata_transactiontype,\
                                   exceptionmetadata_channelproxyerrordata_alertmessage,\
                                   exceptionmetadata_channelproxyerrordata_sequencenumber,\
                                   exceptionmetadata_channelproxyerrordata_alertcode,\
                                   exceptionmetadata_channelproxyerrordata_requestuuid,\
                                   exceptionmetadata_channelproxyerrordata_msgnbr,\
                                   exceptionmetadata_channelproxyerrordata_requestdate,\
                                   exceptionmetadata_channelproxyerrordata_terminal,\
                                   exceptionmetadata_channelproxyerrordata_faultcode,\
                                   exceptionmetadata_channelproxyerrordata_faultactor,\
                                   exceptionmetadata_channelproxyerrordata_faultstring,\
                                   exceptionmetadata_servicename,exceptionmetadata_channelproxyerrorcode,\
                                   exceptionmetadata_channelproxyerrorkey,\
                                   exceptionmetadata_mahtaberrorcode,\
                                   exceptionmetadata_mahtaberrorcode_exceptioncodetype,\
                                   exceptionmetadata_mahtaberrorcode_exceptioncodevalue,\
                                   exceptionmetadata_transactionscount,\
                                   exceptionmetadata_citizenshiptype,\
                                   exceptionmetadata_customertype,exceptionmetadata_shahabcode,\
                                   exceptionmetadata_startdate,exceptionmetadata_count,\
                                   exceptionmetadata_clientcorrelationid,\
                                   exceptionmetadata_requesterbranchcode,exceptionmetadata_boursetype,\
                                   exceptionmetadata_validatecardbymobilenorequestdto,\
                                   exceptionmetadata_blockentrycantbeorempty_,\
                                   exceptionmetadata_activeaccountstockrequestdto,\
                                   exceptionmetadata_dwhstatementerrordata      ,\
                                   exceptionmetadata_documentnumber             ,\
                                   exceptionmetadata_wageinformationvalid       ,\
                                   exceptionmetadata_todate                     ,\
                                   exceptionmetadata_maximumpagesize            ,\
                                   exceptionmetadata_unsupportedpagesizevalue   ,\
                                   exceptionmetadata_paging_pagenumber          ,\
                                   exceptionmetadata_pan                        ,\
                                   exceptionmetadata_daterangevalid             ,\
                                   exceptionmetadata_recipients                 ,\
                                   exceptionmetadata_status                     ,\
                                   exceptionmetadata_requestedtransactioncoun   ,\
                                   exceptionmetadata_minimumtransactioncount    ,\
                                   exceptionmetadata_fct_id                     ,\
                                   exceptionmetadata_acceptor_wage) \
                                   values(:1,:2,:3,:4,:5,:6,:7,:8,:9,:10,:11,:12,:13,:14,:15,:16,:17,:18,\
                                   :19,:20,:21,:22,:23,:24,:25,:26,:27,:28,:29,:30,:31,:32,:33,:34,:35,\
                                   :36,:37,:38,:39,:40,:41,:42,:43,:44,:45,:46,:47,:48,:49,:50,:51,:52,\
                                   :53,:54,:55,:56,:57,:58,:59,:60,:61)",
                                   [str(row['id_']),
                                    str(row['timestamp_']) if row['timestamp_'] != '' else "None",
                                    int(row['id']),
                                    str(row['session_requestid']) if row['session_requestid'] != '' else "None",
                                    str(row['exceptionmetadata_systemname']) if row['exceptionmetadata_systemname'] \
                                                                                != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_transactiontype']) if \
                                        row['exceptionmetadata_mahtaberrordata_transactiontype'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_tracenumber']) if \
                                        row['exceptionmetadata_mahtaberrordata_tracenumber'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_referencenumber']) if \
                                        row['exceptionmetadata_mahtaberrordata_referencenumber'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_requestuuid']) if \
                                        row['exceptionmetadata_mahtaberrordata_requestuuid'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_requestdate']) if \
                                        row['exceptionmetadata_mahtaberrordata_requestdate'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_errorcode']) if \
                                        row['exceptionmetadata_mahtaberrordata_errorcode'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrordata_mahtabdescription']) if \
                                        row['exceptionmetadata_mahtaberrordata_mahtabdescription'] != '' else "None",
                                    str(row['exceptionmetadata_checksource']) if \
                                        row['exceptionmetadata_checksource'] != '' else "None",
                                    str(row['exceptionmetadata_accountnumber']) if \
                                        row['exceptionmetadata_accountnumber'] != '' else "None",
                                    str(row['exceptionmetadata_validationtype']) if \
                                        row['exceptionmetadata_validationtype'] != '' else "None",
                                    str(row['exceptionmetadata_problem']) if \
                                        row['exceptionmetadata_problem'] != '' else "None",
                                    str(row['exceptionmetadata_fieldname']) if \
                                        row['exceptionmetadata_fieldname'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_transactiontype']) if \
                                        row[
                                            'exceptionmetadata_channelproxyerrordata_transactiontype'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_alertmessage']) if \
                                        row['exceptionmetadata_channelproxyerrordata_alertmessage'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_sequencenumber']) if \
                                        row['exceptionmetadata_channelproxyerrordata_sequencenumber'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_alertcode']) if \
                                        row['exceptionmetadata_channelproxyerrordata_alertcode'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_requestuuid']) if \
                                        row['exceptionmetadata_channelproxyerrordata_requestuuid'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_msgnbr']) if \
                                        row['exceptionmetadata_channelproxyerrordata_msgnbr'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_requestdate']) if \
                                        str(row[
                                                'exceptionmetadata_channelproxyerrordata_requestdate']) != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_terminal']) if \
                                        row['exceptionmetadata_channelproxyerrordata_terminal'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_faultcode']) if \
                                        row['exceptionmetadata_channelproxyerrordata_faultcode'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_faultactor']) if \
                                        row['exceptionmetadata_channelproxyerrordata_faultactor'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrordata_faultstring']) if \
                                        row['exceptionmetadata_channelproxyerrordata_faultstring'] != '' else "None",
                                    str(row['exceptionmetadata_servicename']) if \
                                        row['exceptionmetadata_servicename'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrorcode']) if \
                                        row['exceptionmetadata_channelproxyerrorcode'] != '' else "None",
                                    str(row['exceptionmetadata_channelproxyerrorkey']) if \
                                        row['exceptionmetadata_channelproxyerrorkey'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrorcode']) if \
                                        row['exceptionmetadata_mahtaberrorcode'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrorcode_exceptioncodetype']) if \
                                        row['exceptionmetadata_mahtaberrorcode_exceptioncodetype'] != '' else "None",
                                    str(row['exceptionmetadata_mahtaberrorcode_exceptioncodevalue']) if \
                                        row['exceptionmetadata_mahtaberrorcode_exceptioncodevalue'] != '' else "None",
                                    # Commented by reza
                                    # str(row['exceptionmetadata_transactions3']),
                                    # str(row['exceptionmetadata_transactions2']),
                                    str(row['exceptionmetadata_transactionscount']) if \
                                        row['exceptionmetadata_transactionscount'] != '' else "None",
                                    str(row['exceptionmetadata_citizenshiptype']) if \
                                        row['exceptionmetadata_citizenshiptype'] != '' else "None",
                                    str(row['exceptionmetadata_customertype']) if \
                                        row['exceptionmetadata_customertype'] != '' else "None",
                                    str(row['exceptionmetadata_shahabcode']) if \
                                        row['exceptionmetadata_shahabcode'] != '' else "None",
                                    str(row['exceptionmetadata_startdate']) if \
                                        row['exceptionmetadata_startdate'] != '' else "None",
                                    str(row['exceptionmetadata_count']) if \
                                        row['exceptionmetadata_count'] != '' else "None",
                                    str(row['exceptionmetadata_clientcorrelationid']) if \
                                        row['exceptionmetadata_clientcorrelationid'] != '' else "None",
                                    str(row['exceptionmetadata_requesterbranchcode']) if \
                                        row['exceptionmetadata_requesterbranchcode'] != '' else "None",
                                    str(row['exceptionmetadata_boursetype']) if \
                                        row['exceptionmetadata_boursetype'] != '' else "None",
                                    str(row['exceptionmetadata_validatecardbymobilenorequestdto']) if \
                                        row['exceptionmetadata_validatecardbymobilenorequestdto'] != '' else "None",
                                    str(row['exceptionmetadata_blockentrycantbeorempty_']) if \
                                        row['exceptionmetadata_blockentrycantbeorempty_'] != '' else "None",
                                    str(row['exceptionmetadata_activeaccountstockrequestdto']) if \
                                        row['exceptionmetadata_activeaccountstockrequestdto'] != '' else "None",
                                    str(row['exceptionmetadata_dwhstatementerrordata']) if \
                                        row['exceptionmetadata_dwhstatementerrordata'] != '' else "None",
                                    str(row['exceptionmetadata_documentnumber']) if \
                                        row['exceptionmetadata_documentnumber'] != '' else "None",
                                    str(row['exceptionmetadata_wageinformationvalid']) if \
                                        row['exceptionmetadata_wageinformationvalid'] != '' else "None",
                                    str(row['exceptionmetadata_todate']) if \
                                        row['exceptionmetadata_todate'] != '' else "None",
                                    str(int(row['exceptionmetadata_maximumpagesize'])) if \
                                        row['exceptionmetadata_maximumpagesize'] != '' else "None",
                                    str(row['exceptionmetadata_unsupportedpagesizevalue']) if \
                                        row['exceptionmetadata_unsupportedpagesizevalue'] != '' else "None",
                                    str(row['exceptionmetadata_paging_pagenumber']) if \
                                        row['exceptionmetadata_paging_pagenumber'] != '' else "None",
                                    str(row['exceptionmetadata_pan']) if row['exceptionmetadata_pan'] != '' else "None",
                                    str(row['exceptionmetadata_daterangevalid']) if \
                                        row['exceptionmetadata_daterangevalid'] != '' else "None",
                                    str(row['exceptionmetadata_recipients']) if \
                                        row['exceptionmetadata_recipients'] != '' else "None",
                                    str(int(row['exceptionmetadata_status'])) if row['exceptionmetadata_status'] \
                                                                                 != '' else "None",
                                    str(row['exceptionmetadata_requestedtransactioncoun']) \
                                        if row['exceptionmetadata_requestedtransactioncoun'] != '' else "None",
                                    str(int(row['exceptionmetadata_minimumtransactioncount'])) if \
                                        row['exceptionmetadata_minimumtransactioncount'] != '' else "None",
                                    str(int(row['exceptionmetadata_fct_id'])) \
                                        if row['exceptionmetadata_fct_id'] != '' else "None",
                                    str(row['exceptionmetadata_acceptor_wage']) \
                                        if row['exceptionmetadata_acceptor_wage'] != '' else "None"])
                    list_id.append(int(row['id']))

                except Exception as err:

                    try:
                        cursor.execute(
                            "update galaxy_ai.event_prim set except_flag=:1 where id=:2"
                            , [10, int(row['id'])])

                    except Exception as e:
                        print(e)
                        logging.info(e)

                    print(err)
                    print("\n roll back except flag to 10")
                    logging.info(err)

            ####### New version to resolving oracle list length issue: ################

            j = 0
            res = 0
            while list_id:
                batch_ids = list_id[:1000]
                if len(batch_ids) % 1000 == 0:
                    j = j + 1
                else:
                    res = len(batch_ids)

                bind_names = ",".join(":" + str(i + 1) for i in range(len(batch_ids)))
                try:
                    cursor.execute(
                        """UPDATE galaxy_ai.event_prim SET except_flag = 1 WHERE id IN ({})""".format(bind_names),
                        batch_ids
                    )
                    print("Success: Updated except_flag for {} rows.".format(len(batch_ids)))
                    list_id = list_id[1000:]


                except Exception as e:
                    print(e)
                    logging.info(e)

            print(" success=> update except transfer flag to 1 for: " + str(j * 1000 + res))
        except Exception as e:
            print(e)
            print("\n ora cursor conn exception for ****insertOracle_Except_Meta*** ")
            logging.info(e)


        finally:

            conn.commit()
def dict_key_rplc(k):
    return k.replace(' ', '')
def change_keys(obj, dict_key_rplc):
    """
    Recursivly goes through the dictionnary obj and replaces keys with the change_keys function.
    """
    if isinstance(obj, dict):
        new = {}
        for k, v in obj.items():
            new[dict_key_rplc(k)] = change_keys(v, dict_key_rplc)
    else:
        return obj
    return new


async def transferDataDB(conn):
    while True:
        t1 = t.time()
        ds = getExceptMeta(conn)

        lst = list(ds)
        if lst:
            list_meta = []
            for sub in lst:
                l = list(sub)
                try:
                    if l[2] is not None and l[2] != '{}' and l[2] != 'None' and l[2] is not np.nan and \
                            len(l[2]) != 0 and l[2] != 'null':  # and "\"title\":\"429 Too Many Requests" not in l[2] :
                        l[2] = l[2].replace('null(null)', 'null')
                        l[2] = l[2].replace('\"null\"', 'null')
                        l[2] = l[2].replace('null', '\"\"')
                        # l[2]=l[2].replace(' ','')
                        l[2] = l[2].replace('false,', '\"false\",')
                        l[2] = l[2].replace('true,', '\"true\",')
                        l[2] = l[2].replace('problem', 'Problem')
                        l[2] = l[2].replace('accountNumber', 'AccountNumber')
                        l[2] = l[2].replace('nationalCode', 'nationalcode')
                        l[2] = l[2].replace('customerNumber', 'customernumber')
                        l[2] = change_keys(eval(l[2]), dict_key_rplc)
                        my_dict = {'ID_': l[0], 'TIMESTAMP_': l[1], 'EXCEPTIONMETADATA': l[2], 'ID': l[3],
                                   'SESSION_REQUESTID': l[4]}
                        list_meta.append(my_dict)

                except Exception as e:
                    print(e)
                    print(sub)
                    logging.info(e)

            if list_meta:
                df_norm = pd.json_normalize(list_meta)
                df_norm.columns = df_norm.columns.str.replace('.', '_')
                df_norm.columns = df_norm.columns.str.replace('\'', '')
                df_norm.columns = df_norm.columns.str.lower()

                df_except_taraz = pd.DataFrame(columns=['id_', 'timestamp_', 'id', 'session_requestid',
                                                        'exceptionmetadata_systemname',
                                                        'exceptionmetadata_mahtaberrordata_transactiontype',
                                                        'exceptionmetadata_mahtaberrordata_tracenumber',
                                                        'exceptionmetadata_mahtaberrordata_referencenumber',
                                                        'exceptionmetadata_mahtaberrordata_requestuuid',
                                                        'exceptionmetadata_mahtaberrordata_requestdate',
                                                        'exceptionmetadata_mahtaberrordata_errorcode',
                                                        'exceptionmetadata_mahtaberrordata_mahtabdescription',
                                                        'exceptionmetadata_checksource',
                                                        'exceptionmetadata_accountnumber',
                                                        'exceptionmetadata_validationtype',
                                                        'exceptionmetadata_problem',
                                                        'exceptionmetadata_fieldname',
                                                        'exceptionmetadata_channelproxyerrordata_transactiontype',
                                                        'exceptionmetadata_channelproxyerrordata_alertmessage',
                                                        'exceptionmetadata_channelproxyerrordata_sequencenumber',
                                                        'exceptionmetadata_channelproxyerrordata_alertcode',
                                                        'exceptionmetadata_channelproxyerrordata_requestuuid',
                                                        'exceptionmetadata_channelproxyerrordata_msgnbr',
                                                        'exceptionmetadata_channelproxyerrordata_requestdate',
                                                        'exceptionmetadata_channelproxyerrordata_terminal',
                                                        'exceptionmetadata_channelproxyerrordata_faultcode',
                                                        'exceptionmetadata_channelproxyerrordata_faultactor',
                                                        'exceptionmetadata_channelproxyerrordata_faultstring',
                                                        'exceptionmetadata_servicename',
                                                        'exceptionmetadata_channelproxyerrorcode',
                                                        'exceptionmetadata_channelproxyerrorkey',
                                                        'exceptionmetadata_mahtaberrorcode',
                                                        'exceptionmetadata_mahtaberrorcode_exceptioncodetype',
                                                        'exceptionmetadata_mahtaberrorcode_exceptioncodevalue',
                                                        # 1402/06/05
                                                        # commented by reza cause of negotiation with Mrs.Panahi:
                                                        # 'exceptionmetadata_transactions3',
                                                        # 'exceptionmetadata_transactions2',
                                                        'exceptionmetadata_transactionscount',
                                                        'exceptionmetadata_citizenshiptype',
                                                        'exceptionmetadata_customertype',
                                                        'exceptionmetadata_shahabcode',
                                                        'exceptionmetadata_startdate',
                                                        'exceptionmetadata_count',
                                                        'exceptionmetadata_clientcorrelationid',
                                                        'exceptionmetadata_requesterbranchcode',
                                                        'exceptionmetadata_boursetype',
                                                        'exceptionmetadata_validatecardbymobilenorequestdto',
                                                        'exceptionmetadata_blockentrycantbeorempty_',
                                                        'exceptionmetadata_activeaccountstockrequestdto'
                                                        # 14020605 == by mr.sheykhlar request == Added by Reza:
                    , 'exceptionmetadata_dwhstatementerrordata'
                    , 'exceptionmetadata_documentnumber'
                    , 'exceptionmetadata_wageinformationvalid'
                    , 'exceptionmetadata_todate'
                    , 'exceptionmetadata_maximumpagesize'
                    , 'exceptionmetadata_unsupportedpagesizevalue'
                    , 'exceptionmetadata_paging_pagenumber'
                    , 'exceptionmetadata_pan'
                    , 'exceptionmetadata_daterangevalid'
                    , 'exceptionmetadata_recipients'
                    , 'exceptionmetadata_status'
                    , 'exceptionmetadata_requestedtransactioncoun'
                    , 'exceptionmetadata_minimumtransactioncount'
                    , 'exceptionmetadata_fct_id'
                    , 'exceptionmetadata_acceptor_wage'
                                                        ])
                try:
                    df_except_taraz[df_norm.columns] = df_norm
                except Exception as e:
                    print(e)
                    logging.info(e)

                df_except_taraz.fillna('', inplace=True)
                print("before insert")
                insertOracle_Except_Meta(df_except_taraz, conn)

        print(t.time() - t1)
        await asyncio.sleep(10)


db = OraDB(IP, SERVICE)
conn = db.oraConnect()

print('Starting ETL-Exceptions')
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






