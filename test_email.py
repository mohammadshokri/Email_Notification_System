import smtplib
from email.mime.text import MIMEText

mail_server = 'webmail.tiddev.com'
port = 25
username = 'obs.noti@tiddev.com'
password = "R7tZEh3!+#EG%IIM"


server = smtplib.SMTP(mail_server, port)
server.starttls()
server.login(username, password)
msg = MIMEText('Hello, this is a test email.')
msg['Subject'] = 'Test Email'
msg['From'] = 'obs.noti@tiddev.com'

msg['To'] = 'shokri.m@tiddev.com'

server.ehlo()

server.sendmail(msg['From'], msg['To'], msg.as_string())