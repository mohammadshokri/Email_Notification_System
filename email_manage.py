import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart

smtp_server = "webmail.tiddev.com"
msg = MIMEMultipart()
msg['From'] = "obs.noti@tiddev.com"
msg['To'] = "shokri.m@tiddev.com"
msg['Subject'] = "Python SMTP"
smtp_port = 25
message = "Hi this is a test of sending message by python!"
msg.attach(MIMEText(message, 'plain'))
try:
    # smtplib.SMTP_SSL()
    server = smtplib.SMTP(smtp_server, smtp_port)
    server.ehlo()
    server.starttls()
    print('ssl')

    server.login(user="obs.noti", password="DRg^sT%B^c&59_r&")
    print('sending now...')
    server.sendmail(msg['From'], msg['To'], msg.as_string())
    print("Email sent successfully.")
except Exception as e:
    print(f"An error occurred: {e}")
finally:
    if "server" in locals():
        server.quit()