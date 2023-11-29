import  cx_Oracle
import Connectors
from Smtp_conf import SMTPClient, EmailSender
from recipient import load_data_from_csv
from Message import CreateMessage
import schedule
import time as mytime
from datetime import datetime
import jdatetime
roles, people = load_data_from_csv()
smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)

def report_mng_daily():
    print("Report_mng_daily run!")
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT t_date,total_event,success_event,unsuccess_event,success_event_perc,unsuccess_event_perc FROM galaxy_ai.VW_NOTIF_STATUS_MNG ")
        row = cursor.fetchone()  # Use fetchone to get a single row

        if row:
            t_date, total_event, success_event, unsuccess_event, success_event_perc, unsuccess_event_perc = row
            fromDate = jdatetime.datetime.now().strftime("%Y-%m-%d").__str__()+' 00:00 AM'
            toDate = jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()

        cursor.execute("select  SERVICe_NAME, CNT from galaxy_ai.VW_TOP5_SRVICENAME_EXCEP")
        rows = cursor.fetchall()  # Use fetchone to get a single row
        serviceData = {}
        for row in rows:
            service_name, cnt = row
            serviceData[service_name] = cnt

        cursor.execute("select   STATUSCODE,STATUS_DESCRIPTION, CNT, PERCENTAGE from galaxy_ai.VW_NOTIF_EXCEP_STATUSCODE_MNG")
        rows = cursor.fetchall()  # Use fetchone to get a single row
        exceptData = {}
        for row in rows:
            ex_statuscode, ex_status_description, ex_cnt, ex_percentage = row
            exceptData[ex_statuscode] = {
                "STATUS_DESCRIPTION": ex_status_description,
                "CNT": ex_cnt,
                "PERCENTAGE": ex_percentage
            }

        cursor.execute("select CONSUMER, CNT from galaxy_ai.VW_TOP5_CLIENT_EXCEP")
        rows = cursor.fetchall()  # Use fetchone to get a single row
        clientExceptData = {}
        for row in rows:
            consumer, consumer_cnt = row
            clientExceptData[consumer] = consumer_cnt

        event_message = CreateMessage.ReportManagementTemplate(
            fromDate=fromDate,
            toDate=toDate,
            total=total_event,
            succ=success_event,
            unSucc=unsuccess_event,
            succPerc=success_event_perc,
            unSuccPerc=unsuccess_event_perc,
            serviceData = serviceData,
            exceptData = exceptData,
            clientExceptData=clientExceptData
            )
        print(event_message)
        email_sender.send_notification([person.email for person in roles['TOPMANAGEMENT'].members], event_message,
                                           "DOP, Management Reports")

    except cx_Oracle.Error as error:
        print(f"Error: {error}")
    finally:
        cursor.close()

def report_10():
    print("Function report_10 is executed!")


time_function_mapping = {
    "11:07": report_mng_daily,
    "17:11": report_10,

}
#
now = datetime.now()
initial_delays = {time: (datetime(now.year, now.month, now.day, *map(int, time.split(':'))) - now).total_seconds() for time in time_function_mapping}

for time, func in time_function_mapping.items():
    schedule.every().day.at(time).do(func)

min_initial_delay = min(initial_delays.values())


if min_initial_delay < 0:
    min_initial_delay += 24 * 60 * 60  # 24 hours in seconds

mytime.sleep(min_initial_delay)
connection = Connectors.oracle_connect()

while True:
    schedule.run_pending()
    mytime.sleep(1)

# report_mng_daily()


