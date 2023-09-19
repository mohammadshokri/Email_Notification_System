import cx_Oracle
from pathlib import Path
import os
from abc import ABC, abstractmethod

class DatabaseConnector(ABC):
    def __init__(self, connection_string):
        self.connection_string = connection_string

    @abstractmethod
    def connect(self):
        pass

def oracle_connect():
    os.environ["PATH"] = r"D:\instantclient_21_7;" + os.environ["PATH"]
    os.environ["ORACLE_HOME"] = r"D:\instantclient_21_7"

    dsn_tns = cx_Oracle.makedsn('localhost', '1521',
                                service_name='xepdb1')
    conn = cx_Oracle.connect(user=r'uname', password='pass', dsn=dsn_tns)
    return conn

def oracle_dw_connect():
    os.environ["PATH"] = r"D:\instantclient_21_7;" + os.environ["PATH"]
    os.environ["ORACLE_HOME"] = r"D:\instantclient_21_7"

    dsn_tns = cx_Oracle.makedsn('192.168.1.1', '1521',
                                service_name='xepdb2')
    conn = cx_Oracle.connect(user=r'uname', password='****', dsn=dsn_tns)
    return conn

class MongoConnector(DatabaseConnector):
    def connect(self):
        pass