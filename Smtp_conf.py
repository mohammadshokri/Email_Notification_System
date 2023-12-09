import smtplib
import time
from email.mime.text import MIMEText
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import jdatetime
from recipient import load_data_from_csv

roles, people = load_data_from_csv()
class SenderType(ABC):
    @abstractmethod
    def send_notification(self, recipient, message):
        pass

class EmailSender(SenderType):
    def __init__(self, smtp_client):
        self.smtp_client = smtp_client

    def send_notification(self, recipient, message, subject):
        self.smtp_client.send_email(recipient, message, subject)


class SMTPClient:
    def __init__(self, smtp_server="mail.tejaratbank.ir", smtp_port=587,sender_user="dop.notification@tejaratbank.ir", smtp_user="dop.notification", smtp_password="ms9Mmk8#@12s"):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_user = sender_user
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password

    def send_email(self, to_email, message, subject):
        rep_time = f'<br><hr>Reported time {jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()}'
        msg = MIMEText(message + rep_time , _subtype='html', _charset='utf-8' )
        msg.add_header('Content-Type', 'text/html')
        msg['Content-Type'] = 'text/html; charset=utf-8'
        recipient = [person.email for person in roles[to_email].members]
        recipient_str = ', '.join(recipient)
        msg['Subject'] = subject
        msg['From'] = self.sender_user
        msg['To'] = recipient_str
        bcc_emails=''
        if to_email== 'Support':
            bcc_emails = 'panahi.s@tiddev.com'
        elif to_email== 'TOPMANAGEMENT':
            bcc_emails=  'shokri.m@tiddev.com,faghihabdollahi.r@tiddev.com,panahi.s@tiddev.com'

        msg['Bcc'] = bcc_emails

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.ehlo()
            server.starttls()
            server.login(user=self.smtp_user, password=self.smtp_password)
            server.sendmail(msg['From'], [msg['To']] + msg['Bcc'].split(","), msg.as_string())

            server.quit()
            print(f"Email sent to {recipient_str}: {subject}")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
#
# smtp_client = SMTPClient("webmail.tiddev.com", 25, "obs.noti@tiddev.com","obs.noti@tiddev.com", "DRg^sT%B^c&59_r&")
# smtp_client = SMTPClient(smtp_server="mail.tejaratbank.ir", smtp_port=587,sender_user="dop.notification@tejaratbank.ir", smtp_user="dop.notification",smtp_password="ms9Mmk8#@12s")
# email_sender = EmailSender(smtp_client)
# email_sender.send_notification(["shokri.m@tiddev.com"],'message', "Python SMTP")
# email_sender.send_notification([" faghihabdollahi.r@tiddev.com"],'message', "Python SMTP")
