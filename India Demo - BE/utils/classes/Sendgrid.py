"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail, Email, To, Content

from config.config import MailConfig
from typing import Any, Dict, List


class Sendgrid:
    def __init__(self):
        self.API_KEY = MailConfig().SENDGRID_API_KEY
        self.CLIENT = SendGridAPIClient(api_key=self.API_KEY)

    def send(self, receiver: str):
        from_email = Email('test@example.com')
        to_email = To(receiver)
        subject = 'Sending with Sendgrid is fun.'
        content = Content('text/plain', 'and easy to do anywhere, even with Python')
        mail = Mail(from_email=from_email, to_emails=to_email, subject=subject, html_content=content)
        mail_json = mail.get()
        response = self.CLIENT.client.mail.send.post(request_body=mail_json)
        print(response.status_code)
        print(response.headers)
        return response

