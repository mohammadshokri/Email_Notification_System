import  cx_Oracle
from  Smtp_conf import SMTPClient, EmailSender
import Connectors
import threading
import time
from kafka_service import check_kafka_status
from os_services import check_disk_space
from mongo_service import check_mongodb_status
from Message import CreateMessage

smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)

limit_424 = 2000
def chk_424():
    result_dict = {}
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT session_clientid , descr, cnt FROM galaxy_ai.vw_notif_424_error ")

        rows = cursor.fetchall()
        for row in rows:
            session_clientid, descr, cnt = row
            result_dict[session_clientid] = {
                "CNT": cnt,
                "DESCR": descr
            }

        cnt = sum(item["CNT"] for item in result_dict.values())
        print(f'chk_424 is accored---> number of result {cnt}')


        if cnt > limit_424:
            event_message = CreateMessage.EventTemplate(
                totalCount=cnt,
                status='EXCEPTION',
                category='Provider',
                statusCode='424',
                statusName='Failed Dependency',
                statusDesc= 'قطعی سرویس بیرونی'.encode(),
                resultCode='1000021',
                exceptionKey='SERVICE_UNAVAILABLE',
                detail=result_dict)
            email_sender.send_notification('Support', event_message, "DOP event detection: EXCEPTION 424")
            # email_sender.send_notification('ME', event_message, "DOP event detection: EXCEPTION 424")

    except cx_Oracle.Error as error:
        print(f"Error in chk_424 : {error}")
    finally:
        cursor.close()

def chk_timeout():
    connection = Connectors.oracle_connect()
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT COUNT(*) FROM galaxy_ai.all_status")
        count = cursor.fetchone()[0]
        print(f"Count of rows in job_log : {count}")

    except cx_Oracle.Error as error:
        print(f"Error: {error}")
    finally:
        cursor.close()
        connection.close()

def chk_db():
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT dummy from dual" )
    except Exception as e:
        print(f"Error in connecting to database: {e}")
        event_message = CreateMessage.ReportTemplate('Oracle Database','Database is no Responding!',' check database status')
        email_sender.send_notification('Admin', event_message, 'OBS Event: Database Problem')
    finally:
        cursor.close()

def sensor(name, interval, function):
    while True:
        # Call the specified function
        function()
        time.sleep(interval)

# Create a list of sensors with their names, intervals, and functions
sensors = [
    {"name": "Sensor5", "interval": 15*60, "function": check_kafka_status},
    {"name": "Sensor4", "interval": 10*60, "function": check_mongodb_status},
    {"name": "Sensor3", "interval": 10*60, "function": chk_db},
    {"name": "Sensor2", "interval": 15*60, "function": check_disk_space},
    {"name": "Sensor1", "interval": 60*60, "function": chk_424},
]

try:
    connection = Connectors.oracle_connect()

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

except Exception as e:
    print(f"Error in connecting to database: {e}")
    event_message = CreateMessage.ReportTemplate('Problem in Wathing Service', 'check the obs watching service!', '  this is the last Email, Notification is OFF now!<br><strong>CRITICAL STATUS call Admins Mr. Shokri and Mr. Abdolahi</strong>')
    email_sender.send_notification('Admin', event_message,
                                   'DOP Event: Critical Status! Unknown Problem...')
