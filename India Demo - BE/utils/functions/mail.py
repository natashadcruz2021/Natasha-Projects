"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""
import logging

from utils.classes.Job import Job
from utils.classes.SMTPClient import SMTPClient


def create_and_send_mail(job_id: str, email_id: str, file_name: str) -> bool:
    try:
        job = Job()
        sales_order_no = job.fetch_job_detail(id=job_id, param='sales_order_no')
        smtp_client = SMTPClient(email_id=email_id)
        return smtp_client.send(sales_order_no=sales_order_no, file_name=file_name)
    except Exception as e:
        logging.error(f'Create & Send E-mail: Job ID: {job_id} --> Error: {e}')
        return False
