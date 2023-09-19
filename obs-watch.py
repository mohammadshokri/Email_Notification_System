import smtplib
import  cx_Oracle
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from abc import ABC, abstractmethod
from  Smtp_conf import SMTPClient, EmailSender
from recipient import Person,Role
import Connectors
import threading
import time
import random
from recipient import Person,Role,load_data_from_csv
from Message import CreateMessage

roles, people = load_data_from_csv()

smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)
# email_sender.send_notification([moh.email], 'sample context', "Python SMTP")
# email_sender.send_notification([person.email for person in role.members], 'sample context', "Python SMTP")

connection = Connectors.oracle_dw_connect()


def chk_424():

    print("Function Event timeout is running.")
    result_dict = {}
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT session_clientid , count(*) FROM event_prim "\
                       "where  t_date >  to_char(sysdate-10, 'yyyy/mm/dd hh24:mi:ss','nls_calendar=persian')"\
                        # "and status='exception' and eventtype='span' and statuscode='424'"\
                        "GROUP BY session_clientid" )
        rows = cursor.fetchall()

        for row in rows:
            a, b = row
            result_dict[a] = b
        print(result_dict)

        event_message = CreateMessage.EventTemplate(sum(result_dict.values()),'EXCEPTION','??',424,'Failed Dependency','قطعی سرویس بیرونی',1000021,'SERVICE_UNAVAILABLE',result_dict.__str__() )
        email_sender.send_notification([person.email for person in roles['Admin'].members], event_message, "OBS Event: EXCEPTION 424")
    except cx_Oracle.Error as error:
        print(f"Error: {error}")
    finally:
        cursor.close()
def chk_timeout():
    connection = Connectors.oracle_connect()
    print("Function Event timeout is running.")
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM all_status")
        count = cursor.fetchone()[0]
        print(f"Count of rows in job_log : {count}")

    except cx_Oracle.Error as error:
        print(f"Error: {error}")
    finally:
        cursor.close()
        connection.close()

def chk_services ():
    print("Function services status is running.")

def chk_db():
    print("Function Db Check is running.")

def sensor(name, interval, function):
    while True:
        # Call the specified function
        function()
        time.sleep(interval)

# Create a list of sensors with their names, intervals, and functions
sensors = [
    {"name": "Sensor1", "interval": 2, "function": chk_timeout},
    {"name": "Sensor2", "interval": 3, "function": chk_services},
    {"name": "Sensor3", "interval": 10, "function": chk_db},
    {"name": "Sensor3", "interval": 5, "function": chk_424},
]


# Create and start threads for each sensor
threads = []
for sensor_info in sensors:
    name = sensor_info["name"]
    function = sensor_info["function"]
    interval = sensor_info["interval"]
    thread = threading.Thread(target=sensor, args=(name, interval, function))
    threads.append(thread)
    thread.start()

for thread in threads:
    thread.join()
connection.close()