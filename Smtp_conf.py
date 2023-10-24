import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from abc import ABC, abstractmethod
from recipient import Person,Role

class SenderType(ABC):
    @abstractmethod
    def send_notification(self, recipient, message):
        pass

class EmailSender(SenderType):
    def __init__(self, smtp_client):
        self.smtp_client = smtp_client

    def send_notification(self, recipient, message, subject):
        for reci in  recipient:
            print(reci)
            self.smtp_client.send_email(reci, message, subject)

class SMTPClient:
    def __init__(self, smtp_server="webmail.tiddev.com", smtp_port=25, smtp_user="obs.noti@tiddev.com", smtp_password="DRg^sT%B^c&59_r&"):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password

    def send_email(self, to_email, message, subject):
        msg = MIMEText(message)
        msg['Subject'] = subject
        msg['From'] = self.smtp_user
        msg['To'] = to_email

        try:
            server = smtplib.SMTP(self.smtp_server, self.smtp_port)
            server.ehlo()
            server.starttls()
            server.login(user=self.smtp_user, password=self.smtp_password)
            server.sendmail(msg['From'], msg['To'], msg.as_string())
            server.quit()
            print(f"Email sent to {to_email}: {subject}")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
#
# smtp_client = SMTPClient("webmail.tiddev.com", 25, "obs.noti@tiddev.com", "DRg^sT%B^c&59_r&")
# email_sender = EmailSender(smtp_client)
# email_sender.send_notification(["shokri.m@tiddev.com"],'message', "Python SMTP")



