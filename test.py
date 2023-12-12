import smtplib
import base64
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from email.mime.image import MIMEImage
import matplotlib.pyplot as plt
from io import BytesIO
from abc import ABC, abstractmethod
# from bidi.algorithm import get_display
# from arabic_reshaper import reshape

class SenderType(ABC):
    @abstractmethod
    def send_notification(self, recipient, message):
        pass

class EmailSender(SenderType):
    def __init__(self, smtp_client):
        self.smtp_client = smtp_client

    def send_notification(self, recipient, message, subject):
        self.smtp_client.send_email(recipient, message, subject)

    def generate_pie_chart(self, data):
        labels = [name_info.get('DESCR', '') for name_info in data]
        # persian_labels = [get_display(reshape(label)) for label in labels]
        counts = [name_info.get('CNT', 0) for name_info in data]

        # plt.pie(counts, labels=persian_labels, autopct='%1.1f%%', startangle=140)
        # plt.axis('equal')  # Equal aspect ratio ensures that pie is drawn as a circle.


        # plt.title("chart_title", fontdict={'fontsize': 16}, y=1.05)
        # plt.tight_layout(pad=2.0)
        # chart_buffer = BytesIO()
        # plt.savefig(chart_buffer, format='png')
        # plt.close()

        # Create a MIMEImage object and attach it to the email
        # chart_image = MIMEImage(chart_buffer.getvalue(), name='chart.png')
        # chart_image.add_header('Content-Disposition', 'attachment', filename='chart.png')

        return chart_image

    def send_notification(self, recipient, message, subject, chart_data=None):
        # Create a multipart message
        msg = MIMEMultipart()

        # Add the HTML body to the message
        body = MIMEText(message + '<br>', _subtype='html', _charset='utf-8')
        msg.attach(body)

        # Add the chart image to the message if chart_data is provided
        # if chart_data:
            # chart_image = self.generate_pie_chart(chart_data)
            # msg.attach(chart_image)

        # Add additional headers
        msg['Subject'] = subject
        msg['From'] = self.smtp_client.sender_user
        msg['To'] = ', '.join(recipient)

        # Bcc emails logic here...

        try:
            server = smtplib.SMTP(self.smtp_client.smtp_server, self.smtp_client.smtp_port)
            server.ehlo()
            server.starttls()
            server.login(user=self.smtp_client.smtp_user, password=self.smtp_client.smtp_password)
            server.sendmail(msg['From'], recipient, msg.as_string())

            server.quit()
            print(f"Email sent to {', '.join(recipient)}: {subject}")
        except Exception as e:
            print(f"Error sending email: {str(e)}")


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

data_set = [
    {"Service Name": "Core.Transfer.AccountToGL.Do", "DESCR": "توضیح  1", "CNT": 800},
    {"Service Name": "Core.Transfer", "DESCR": "توضیح شماره دو 2", "CNT": 50},
    {"Service Name": "Core.AccountToGL.Do", "DESCR": "describe 3", "CNT": 150},
    # ... (other data)
]



smtp_client = SMTPClient("webmail.tiddev.com", 25, "obs.noti@tiddev.com","obs.noti@tiddev.com", "DRg^sT%B^c59_r")
email_sender = EmailSender(smtp_client)

email_sender.send_notification(["shokri.m@tiddev.com"], 'message', "Python SMTP", chart_data=data_set)
