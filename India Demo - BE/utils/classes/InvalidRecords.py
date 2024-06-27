"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from schemas.PostgreSQL import PostgreSQL
from utils.classes.Job import Job


class InvalidRecords:
    def __init__(self, job_id: str):
        self.job_id = job_id

    def fetch_invalid_records(self):
        job = Job()
        job_details = job.fetch_job_detail(self.job_id)
        table_name = job_details['table_name']
        pg = PostgreSQL()
        pg.fetch_entries(table_name=table_name)
        pass

    def create_file(self):
        pass
