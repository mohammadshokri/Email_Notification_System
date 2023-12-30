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
connection = Connectors.oracle_connect()
def report_mng_daily():
    print("Report_mng_daily run!")
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT /*+ parallel(a 10)*/ t_date,total_event,success_event,unsuccess_event,success_event_perc,unsuccess_event_perc FROM galaxy_ai.VW_NOTIF_STATUS_MNG a")
        row = cursor.fetchone()  # Use fetchone to get a single row

        if row:
            t_date, total_event, success_event, unsuccess_event, success_event_perc, unsuccess_event_perc = row
            fromDate = jdatetime.datetime.now().strftime("%Y-%m-%d").__str__()+' 00:00 AM'
            toDate = jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()

        cursor.execute("select  /*+ parallel(a 10)*/ SERVICe_NAME, CNT, DESCR from galaxy_ai.VW_TOP5_SRVICENAME_EXCEP a ")
        rows = cursor.fetchall()  # Use fetchone to get a single row
        serviceData = {}
        for row in rows:
            service_name, service_cnt,service_descr = row
            serviceData[service_name] = {
                 "CNT": service_cnt,
                 "DESCR" : service_descr}

        cursor.execute("select /*+ parallel(a 10)*/  STATUSCODE,STATUS_DESCRIPTION, CNT, PERCENTAGE from galaxy_ai.VW_NOTIF_EXCEP_STATUSCODE_MNG a ")
        rows = cursor.fetchall()
        exceptData = {}
        for row in rows:
            ex_statuscode, ex_status_description, ex_cnt, ex_percentage = row
            exceptData[ex_statuscode] = {
                "STATUS_DESCRIPTION": ex_status_description,
                "CNT": ex_cnt,
                "PERCENTAGE": ex_percentage
            }
        cursor.execute("select /*+ parallel(a 10)*/  CONSUMER, CNT, DESCR from galaxy_ai.VW_TOP5_CLIENT_EXCEP a ")
        rows = cursor.fetchall()
        clientExceptData = {}
        for row in rows:
            consumer, consumer_cnt,  consumer_descr= row
            clientExceptData[consumer] = {
                 "CNT": consumer_cnt,
                 "DESCR" : consumer_descr}

        # fromDate = None
        # toDate = None
        # total_event = None
        # success_event = None
        # unsuccess_event = None
        # success_event_perc = None
        # unsuccess_event_perc = None
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

        email_sender.send_notification('TOPMANAGEMENT', event_message, "DOP, Management Reports", chart_data=clientExceptData)
        # email_sender.send_notification('ME', event_message, "DOP, Management Reports", chart_data=clientExceptData)

    except Exception as e:
        print(f"Error accord : {e}")
    finally:
        cursor.close()

def report_10():
    print("Function report_10 is executed!")


def abnormalTableDetection():
    print("Check Table Abnormality !")
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT NAME, TYPE, DESCR, LVL, SUBJECT FROM dop_noti.tb_incident WHERE status = 1")

        for row in cursor.fetchall():
            name, type, descr, lvl, subject = row
            event_message = CreateMessage.AbnormalityTemplate(
                name=name,
                type=type,
                descr=descr,
                lvl=lvl,
                subject=subject
            )
            email_sender.send_notification('ME', event_message, "DOP, Incident")

    except Exception as e:
        print(f"Error occurred in abnormality detection: {e}")
    finally:
        cursor.close()


# Example usage
# abnormalTableDetection()
# report_mng_daily()


schedule.every().day.at("22:00").do(report_mng_daily)


# schedule.every().day.at("10:30").do(job)
# schedule.every().monday.do(job)
# schedule.every().wednesday.at("13:15").do(job)


while True:
    schedule.run_pending()
    mytime.sleep(10)

