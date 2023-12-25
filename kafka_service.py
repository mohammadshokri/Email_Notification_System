from  Smtp_conf import SMTPClient, EmailSender
from Message import CreateMessage
from recipient import load_data_from_csv

roles, people = load_data_from_csv()

bootstrap_servers = ['10.40.195.158:9092']
# bootstrap_servers = ['192.168.102.72:9092']

smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)

from kafka import KafkaAdminClient
import subprocess

topics_status = {'EventTopic': {'status': True}, 'LogstashTopic': {'status': True}}

def start_kafka_service():
    try:
        kafka_service_name = 'kafka'
        subprocess.run(['sudo', 'systemctl', 'start', kafka_service_name], check=True)
        print(f"Kafka service ({kafka_service_name}) started successfully.")
    except subprocess.CalledProcessError as e:
        print(f"Error starting Kafka service: {e}")
def check_kafka_status():
    try:
        admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        available_topics = admin_client.list_topics()

        for topic_name, expected_status in topics_status.items():
            if topic_name in available_topics:
                print(f"Kafka Topic {topic_name} is Up")
                if expected_status['status'] == False:
                    expected_status['status'] = True
                    event_message = CreateMessage.ReportTemplate('Resolved-Kafka', 'Up',
                                                                 f"Kafka {topic_name} is Up now.")
                    email_sender.send_notification('Admin', event_message, f"DOP Event: Infra Broken - {topic_name}")
            else:
                print(f"Kafka Topic {topic_name} is down")
                expected_status['status'] = False
                event_message = CreateMessage.ReportTemplate('Critical-Kafka', 'Down',
                                                             f"Kafka {topic_name} is Down.")
                email_sender.send_notification('Admin', event_message,
                                               f"DOP Event: Infra Broken - {topic_name}")
                start_kafka_service()

    except Exception as e:
        print(f"Kafka error: {e}")
        for topic_name, expected_status in topics_status.items():
            expected_status['status'] = False
            event_message = CreateMessage.ReportTemplate('Critical-Kafka', 'Unknown Status',
                                                         f"Kafka {topic_name} is in an unknown state.")
            email_sender.send_notification('Admin', event_message,
                                           f"DOP Event: Infra Broken - {topic_name}")
            start_kafka_service()

# Usage example
check_kafka_status()

