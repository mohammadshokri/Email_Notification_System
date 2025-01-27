import smtplib
import time
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from abc import ABC, abstractmethod
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import jdatetime
import matplotlib.pyplot as plt
from io import BytesIO
from recipient import load_data_from_csv
from bidi.algorithm import get_display
from arabic_reshaper import reshape
import logging

logging.basicConfig(filename='Smtp_conf_log.log', level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
roles, people = load_data_from_csv()
class SenderType(ABC):
    @abstractmethod
    def send_notification(self, recipient, message):
        pass

class EmailSender(SenderType):
    def __init__(self, smtp_client):
        self.smtp_client = smtp_client

    def send_notification(self, recipient, message, subject,chart_data=None):
        self.smtp_client.send_email(recipient, message, subject,chart_data)

class SMTPClient:
    def __init__(self, smtp_server="mail.tejaratbank.ir", smtp_port=587,sender_user="dop.notification@tejaratbank.ir", smtp_user="dop.notification", smtp_password="ms87moh@#s"):
        self.smtp_server = smtp_server
        self.smtp_port = smtp_port
        self.sender_user = sender_user
        self.smtp_user = smtp_user
        self.smtp_password = smtp_password

    def generate_pie_chart(self, clientExceptData):
        labels = [client_info['DESCR'] for client_info in clientExceptData.values()]
        # labels = [client_info.get('CONSUMER', '') for client_info in clientExceptData.values()]
        persian_labels = [get_display(reshape(label)) for label in labels]
        counts = [float(client_info['CNT'].replace(',', '')) for client_info in clientExceptData.values()]

        plt.pie(counts, labels=persian_labels, autopct='%1.1f%%', startangle=140)
        plt.axis('equal')
        plt.title("Top 5 Clients with Most Errors", fontdict={'fontsize': 16}, y=1.05)
        plt.tight_layout(pad=2.0)
        chart_buffer = BytesIO()
        plt.savefig(chart_buffer, format='png')
        plt.close()

        chart_image = MIMEImage(chart_buffer.getvalue(), name='chart.png')
        chart_image.add_header('Content-Disposition', 'attachment', filename='chart.png')

        return chart_image
    def send_email(self, to_email, message, subject,chart_data):

        msg = MIMEMultipart()
        body = MIMEText(message , _subtype='html', _charset='utf-8')
        msg.attach(body)

        if chart_data:
            chart_image = self.generate_pie_chart(chart_data)
            msg.attach(chart_image)

        msg.add_header('Content-Type',  'text/html')
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
            server.sendmail(msg['From'], msg['To'].split(",") + msg['Bcc'].split(","), msg.as_string())

            server.quit()
            print(f"Email sent to {recipient_str}: {subject}")
            logging.info(f"Email {subject} sent to {recipient_str} ")
        except Exception as e:
            print(f"Error sending email: {str(e)}")
            logging.error(f"Error sending {subject} email to {recipient_str} : {str(e)}")
#

# smtp_client = SMTPClient(smtp_server="webmail.tiddev.com", smtp_port=25,sender_user="obs.noti@tiddev.com", smtp_user="obs.noti@tiddev.com",smtp_password="R7tZEh3!+#EG%IIM")
#smtp_client = SMTPClient(smtp_server="mail.tejaratbank.ir", smtp_port=587,sender_user="dop.notification@tejaratbank.ir", smtp_user="dop.notification",smtp_password="ms87moh@#s")
# email_sender = EmailSender(smtp_client)
# email_sender.send_notification("Admin",'message', "Python SMTP")
# email_sender.send_notification([" faghihabdollahi.r@tiddev.com"],'message', "Python SMTP")
