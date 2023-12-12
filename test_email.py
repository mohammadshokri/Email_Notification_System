import smtplib
from email.mime.text import MIMEText


mail_server = 'webmail.tiddev.com'
port = 25
username = 'obs.noti@tiddev.com'
password = 'DRg^sT%B^c59_r'
server = smtplib.SMTP(mail_server, port)
server.starttls()
server.login(username, password)
msg = MIMEText('Hello, this is a test email.')
msg['Subject'] = 'Test Email'
msg['From'] = 'obs.noti@tiddev.com'
msg['To'] = 'shokri.m@tiddev.com'

server.sendmail('webmail.tiddev.com', 'obs.noti@tiddev.com', msg.as_string())


server.quit()
