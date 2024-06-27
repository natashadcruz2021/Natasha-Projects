"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""
import logging
from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from flask import current_app
import smtplib
import ssl

from config.config import MailConfig
from constants import INVALID_FOLDER


class SMTPClient:
    def __init__(self, email_id: str):
        self.port = MailConfig.SMTP_PORT    # For SSL
        self.server_address = MailConfig.SMTP_SERVER_ADDRESS
        self.sender_email = MailConfig.SMTP_SENDER_EMAIL
        self.cc_email = 'archit.awasthi@go-yubi.com'
        self.password = MailConfig.SMTP_PASSWORD
        self.receiver_email = email_id

    def send(self, sales_order_no: str, file_name: str) -> bool:
        is_sent = False
        message = MIMEMultipart('alternative')
        part = MIMEBase('application', 'octet-stream')
        part.set_payload(open(f'{current_app.config[INVALID_FOLDER]}/{file_name}', 'rb').read())
        encoders.encode_base64(part)
        part.add_header('Content-Disposition', f'attachment; filename={file_name}')
        message.attach(part)
        message['Subject'] = f'Invalid File for Sales Order No: {sales_order_no}'
        message['From'] = f'Spocto Pvt. Ltd. Notifications <{self.sender_email}>'
        message['To'] = self.sender_email
        message['cc'] = self.cc_email
        email = [self.sender_email] + self.cc_email.split(',')
        html = ''
        part = MIMEText(html, 'html')
        message.attach(part)
        try:
            context = ssl.create_default_context()
            with smtplib.SMTP_SSL(self.server_address, self.port, context=context) as server:
                server.login(self.sender_email, self.password)
                server.sendmail(self.sender_email, email, message.as_string())
                is_sent = True
        except Exception as e:
            logging.error('Error with email %s', str(e))
        return is_sent
