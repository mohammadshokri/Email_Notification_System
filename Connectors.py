import cx_Oracle
import os
from abc import ABC, abstractmethod

class DatabaseConnector(ABC):
    def __init__(self, connection_string):
        self.connection_string = connection_string

    @abstractmethod
    def connect(self):
        pass

def oracle_connect():
    os.environ["PATH"] = r"/oracle/instantclient_21_10;" + os.environ["PATH"]
    os.environ["ORACLE_HOME"] = r"/oracle/instantclient_21_10"

    dsn_tns = cx_Oracle.makedsn('10.40.195.156', '1522',
                                service_name='dw')
    conn = cx_Oracle.connect(user=r'obs_noti', password='obsNoti123', dsn=dsn_tns)
    return conn

class MongoConnector(DatabaseConnector):
    def connect(self):
        pass