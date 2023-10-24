from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

topic_names=['LogstashTopic','EventTopic']
def check_kafka_status(bootstrap_servers):
    for topic_name in topic_names:
        try:
            consumer = KafkaConsumer(topic_name,bootstrap_servers=bootstrap_servers)
        except NoBrokersAvailable:
            print(f"Kafka Topic {topic_name} is down")
        except Exception as e:
            print(f"Kafka error: {e}")

if __name__ == "__main__":
    # bootstrap_servers = ['10.40.195.158:9092']
    bootstrap_servers = ['192.168.102.72:9092']
    check_kafka_status(bootstrap_servers)
