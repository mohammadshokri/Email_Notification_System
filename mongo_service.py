from pymongo import MongoClient
from Message import CreateMessage
from recipient import load_data_from_csv
from  Smtp_conf import SMTPClient, EmailSender

roles, people = load_data_from_csv()
mongo_status = True
smtp_client = SMTPClient()
email_sender = EmailSender(smtp_client)

def check_mongodb_status():
    global mongo_status
    try:
        url = "mongodb://galaxyadmin:galaxydmin153@10.40.195.153:1523/?authMechanism=DEFAULT&authSource=galaxy"
        client = MongoClient(url)

        client.admin.command('ismaster')
        print("MongoDB is up and running.")
        if mongo_status == False:
            mongo_status = True
            event_message = CreateMessage.ReportTemplate('Resolved-DataLake', 'Up',
                                                         f"Mongodb is UP and Available now.")
            email_sender.send_notification([person.email for person in roles['Admin'].members],
                                           event_message, f"OBS Event: Infra Broken - Data lake")
    except Exception as e:
        print(f"MongoDB is not available: {e}")
        mongo_status = False
        event_message = CreateMessage.ReportTemplate('Critical-DataLake', 'Down',
                                                     "Mongodb is Down or unavailable")
        email_sender.send_notification([person.email for person in roles['Admin'].members], event_message,
                                       f"OBS Event: Infra Broken - Data lake")

# Call the function to check MongoDB status
check_mongodb_status()
