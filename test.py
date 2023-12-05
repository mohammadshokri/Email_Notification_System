import smtplib
import time
from email.mime.text import MIMEText
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import jdatetime

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
    def __init__(self, smtp_server, smtp_port, smtp_user, smtp_password):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password

    def send_email(self, to_email, message, subject):
        rep_time = f'<br><hr>Reported time {jdatetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S").__str__()}'
        msg = MIMEText(message + rep_time , _subtype='html', _charset='utf-8' )
        msg.add_header('Content-Type', 'text/html')
        msg['Content-Type'] = 'text/html; charset=utf-8'
        recipient = 'shokri.m@tiddev.com'

        msg['Subject'] = subject
        msg['From'] = self.smtp_user
        msg['To'] = recipient

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.ehlo()
            server.starttls()
            server.login(user=self.smtp_user, password=self.smtp_password)
            server.sendmail(msg['From'], msg['To'], msg.as_string())

            server.quit()
            print(f"Email sent to {recipient}: {subject}")
        except Exception as e:
            print(f"Error sending email: {str(e)}")


smtp_client = SMTPClient(smtp_server="mail.tejaratbank.ir", smtp_port=465,smtp_user="dop.notification@tejaratbank.ir",smtp_password="ms9Mmk8#@12s")
email_sender = EmailSender(smtp_client)
email_sender.send_notification(["shokri.m@tiddev.com"],'tej email', "test email tej")
# email_sender.send_notification([" faghihabdollahi.r@tiddev.com"],'message', "Python SMTP")
