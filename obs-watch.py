import  cx_Oracle
from  Smtp_conf import SMTPClient, EmailSender
import Connectors
import threading
import time
from kafka_service import check_kafka_status
from mongo_service import check_mongodb_status
from recipient import load_data_from_csv
from Message import CreateMessage
roles, people = load_data_from_csv()
import paramiko
smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)
container_names = ["etl-req-resp-docker", "etl-exception-docker", "etl-event-docker", "kafka-event-docker", "kafka-logstash-docker"]
remote_servers = [
    {'name':'Datawarehouse', 'ip': '10.40.195.156', 'username': 'root', 'threshold': 90, 'path' : '/u01'},
    {'name':'DataLake', 'ip': '10.40.195.153', 'username': 'ubuntu','threshold': 60, 'path' : '/'},
    {'name':'Pipeline', 'ip': '10.40.195.158', 'username': 'ubuntu', 'threshold': 60, 'path' : '/'},

    # Add more remote servers as needed
]
limit_424 = 1
def chk_424():
    result_dict = {}
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT session_clientid , count(*) cnt FROM galaxy_ai.tb_error "\
                       "where  t_date >  to_char(sysdate-1/24, 'yyyy/mm/dd hh24:mi:ss','nls_calendar=persian')"\
                        "and statuscode='424'"\
                        "GROUP BY session_clientid" )

        rows = cursor.fetchall()
        for row in rows:
            session_clientid, cnt = row
            result_dict[session_clientid] = cnt

        cnt = sum(result_dict.values())
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
            email_sender.send_notification([person.email for person in roles['ME'].members], event_message, "DOP event detection: EXCEPTION 424")

    except cx_Oracle.Error as error:
        print(f"Error: {error}")
    finally:
        cursor.close()

def check_disk_space():
    for server in remote_servers:
        host = server['ip']
        username = server['username']
        server_name = server['name']
        private_key_path = '/home/ubuntu/.ssh/id_rsa'
        threshold_percent = server['threshold']
        path_to_check =  server['path']

        key = paramiko.RSAKey(filename=private_key_path)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(host, username=username, pkey=key)
            command = f"df -h {path_to_check}"
            stdin, stdout, stderr = client.exec_command(command)

            # Parse the output to get disk space information
            output = stdout.read().decode("utf-8").strip()
            lines = output.split('\n')[1:]
            for line in lines:
                filesystem, size, used, available, percent, mountpoint = line.split()
                if mountpoint == path_to_check:
                    percent_used = int(percent.rstrip('%'))
                    print(f"Disk space on {server_name}  {host}:{path_to_check} - Percentage Used: {percent_used}%")
                    if percent_used > threshold_percent:
                        event_message = CreateMessage.ReportTemplate('Infrastructure', f"Disk Space usage is Low! {percent_used}% Used on [{server_name}]",
                                                         f"Path {path_to_check} - Total Space: {size} GB, Used Space: {used} GB, Free Space: {available} GB, \nPercentage Used: {percent_used}%")
                        email_sender.send_notification([person.email for person in roles['Admin'].members], event_message, "OBS Event: Disk Capacity")
        except Exception as e:
            print(f"Error connecting to {host}: {e}")
        finally:
            # Close the SSH connection
            client.close()

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

def chk_db():
    try:
        cursor = connection.cursor()
        cursor.execute("SELECT dummy from dual" )
    except Exception as e:
        print(f"Error in connecting to database: {e}")
        event_message = CreateMessage.ReportTemplate('Oracle Database','Database is no Responding!',' check database status')
        email_sender.send_notification([person.email for person in roles['Admin'].members], event_message, 'OBS Event: Database Problem')
    finally:
        cursor.close()

def sensor(name, interval, function):
    while True:
        # Call the specified function
        function()
        time.sleep(interval)

# Create a list of sensors with their names, intervals, and functions
sensors = [
    # {"name": "Sensor1", "interval": 60, "function": check_docker_status},
    {"name": "Sensor5", "interval": 15*60, "function": check_kafka_status},
    # {"name": "Sensor4", "interval": 3, "function": chk_services},
    {"name": "Sensor4", "interval": 10*60, "function": check_mongodb_status},
    {"name": "Sensor3", "interval": 10*60, "function": chk_db},
    {"name": "Sensor2", "interval": 15*60, "function": check_disk_space},
    {"name": "Sensor1", "interval": 1*60, "function": chk_424},
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
    event_message = CreateMessage.ReportTemplate('Problem in Wathing Service', 'check the obs watching service!', ' Emergency status')
    email_sender.send_notification([person.email for person in roles['Admin'].members], event_message,
                                   'OBS Event: Unknown Problem, this is the last Email, Notification is OFF now!')
