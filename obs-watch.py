import  cx_Oracle
from  Smtp_conf import SMTPClient, EmailSender
import Connectors
import threading
import time
from recipient import load_data_from_csv
from Message import CreateMessage
import docker
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable
roles, people = load_data_from_csv()


smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)
# email_sender.send_notification([moh.email], 'sample context', "Python SMTP")
# email_sender.send_notification([person.email for person in role.members], 'sample context', "Python SMTP")
container_names = ["etl-req-resp-docker", "etl-exception-docker", "etl-event-docker", "kafka-event-docker", "kafka-logstash-docker"]
topic_names=['LogstashTopic','EventTopic']
bootstrap_servers = ['192.168.102.72:9092']
connection = Connectors.oracle_dw_connect()


def chk_424():

    print("Function Event timeout is running.")
    result_dict = {}
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT session_clientid , count(*) FROM galaxy_ai.event_prim "\
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
        cursor.execute("SELECT COUNT(*) FROM galaxy_ai.all_status")
        count = cursor.fetchone()[0]
        print(f"Count of rows in job_log : {count}")

    except cx_Oracle.Error as error:
        print(f"Error: {error}")
    finally:
        cursor.close()
        connection.close()

def chk_services ():
    print("Function services status is running.")
def put_queue(event, what):
    print(event, what)
'''
def check_docker_status():
    # client = docker.from_env()
    client = docker.DockerClient(base_url=f'tcp://10.40.195.158:2375')
    containers = client.containers.list()
    if containers:
        running_container_names = [container.name for container in containers]
    for container_name in container_names:
        try:
            for name in container_names:
                if name not in running_container_names:
                    event_message = CreateMessage.ReportTemplate('Critical', 'Down', f"Container '{container_name}' is not running.")
                    put_queue('Critical', f"Container '{container_name}' is not running.")
                    # email_sender.send_notification([person.email for person in roles['Manager'].members], event_message,
                    #                        "OBS Event: Infra Broken")
        except Exception as e:
            event_message = CreateMessage.ReportTemplate('Critical', 'Unknown',
                                                         f"Containers are Unknown.")
            email_sender.send_notification([person.email for person in roles['Manager'].members], event_message,
                                           "OBS Event: Infra Exceptions Occured")
'''
def check_kafka_status():
    for topic_name in topic_names:
        try:
            consumer = KafkaConsumer(
                topic_name,
                bootstrap_servers=bootstrap_servers,
                enable_auto_commit=True,
            )
        except NoBrokersAvailable:
            print(f"Kafka Topic {topic_name} is down")
            event_message = CreateMessage.ReportTemplate('Critical-Kafka', 'Down',
                                                         f"Kafka '{topic_name}' is Down.")
            email_sender.send_notification([person.email for person in roles['Manager'].members], event_message,
                                           "OBS Event: Infra Broken")
        except Exception as e:
            print(f"Kafka error: {e}")
def chk_db():
    print("Function Db Check is running.")

def sensor(name, interval, function):
    while True:
        # Call the specified function
        function()
        time.sleep(interval)

# Create a list of sensors with their names, intervals, and functions
sensors = [
    # {"name": "Sensor1", "interval": 60, "function": check_docker_status},
    # {"name": "Sensor1", "interval": 60, "function": check_kafka_status},
    # {"name": "Sensor2", "interval": 3, "function": chk_services},
    # {"name": "Sensor3", "interval": 10, "function": chk_db},
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