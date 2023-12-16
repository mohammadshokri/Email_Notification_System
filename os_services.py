from  Smtp_conf import SMTPClient, EmailSender
import logging
from Message import CreateMessage
logging.basicConfig(filename='os_event.log', level=logging.ERROR, format='%(asctime)s - %(levelname)s - %(message)s')
import paramiko

smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)

container_names = ["etl-req-resp-docker", "etl-exception-docker", "etl-event-docker", "kafka-event-docker", "kafka-logstash-docker"]
remote_servers = [
    {'name':'Datawarehouse', 'ip': '10.40.195.156', 'username': 'root', 'threshold': 90, 'threshold_swap':1, 'path' : '/u01'},
    {'name':'DataLake', 'ip': '10.40.195.153', 'username': 'ubuntu','threshold': 60, 'threshold_swap':10,'path' : '/'},
    {'name':'Pipeline', 'ip': '10.40.195.158', 'username': 'ubuntu', 'threshold': 60, 'threshold_swap':10,'path' : '/'},

    # Add more remote servers as needed
]

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
                        email_sender.send_notification('Admin', event_message, "DOP Event: Disk Capacity")

        except Exception as e:
            print(f"Error connecting to {host}: {e}")
            logging.error(f"Error on Disk Capacity check on {server_name}: {str(e)}")
        finally:
            # Close the SSH connection
            client.close()

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

def check_swap_size():
    for server in remote_servers:
        host = server['ip']
        username = server['username']
        server_name = server['name']
        threshold_swap = server['threshold_swap']
        private_key_path = '/home/ubuntu/.ssh/id_rsa'

        key = paramiko.RSAKey(filename=private_key_path)
        client = paramiko.SSHClient()
        client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            client.connect(host, username=username, pkey=key)
            command = "free -g | grep Swap"
            stdin, stdout, stderr = client.exec_command(command)

            # Parse the output to get swap space information
            output = stdout.read().decode("utf-8").strip()
            total_swap, used_swap, free_swap = map(int, output.split()[1:4])
            print(f"Swap space on {server_name} {host} - Total: {total_swap} G, Used: {used_swap} G, Free: {free_swap} G")


            if used_swap > threshold_swap:
                event_message = CreateMessage.ReportTemplate('Infrastructure', f"Swap Space usage is high on [{server_name}]",
                                                              f"Total Swap: {total_swap} G, Used Swap: {used_swap} G, Free Swap: {free_swap} G")
                email_sender.send_notification('Admin', event_message, "Dop Event: Swap Space")

        except Exception as e:
            print(f"Error connecting to {host}: {e}")
        finally:
            # Close the SSH connection
            client.close()
