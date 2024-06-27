"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import os
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict
from uuid import uuid4

from flask import current_app

from config.config import CustomerDetailsConfig
from constants import ORDER, ALLOCATION_FOLDER
from models.Job import Job as JobModel
from schemas.MongoDB import MongoDB
from utils.classes.CustomerPortfolio import CustomerPortfolio
from utils.functions import convert_bytes


class Job:
    def __init__(self, mongodb: MongoDB = None):
        if mongodb is None:
            self.mongodb = MongoDB()
        else:
            self.mongodb = mongodb

    def fetch_jobs(self, order: str = 'asc', id: str = None, skip: int = 0, limit: int = 0) -> Dict[str, Any]:
        options = {'operation': 'sort', 'col_name': 'modified_date', 'is_active': True}
        if id is not None:
            options['_id'] = id
        job_list = self.mongodb.fetch_entries(collection_name='jobs', filters=options, order=ORDER[order], skip=skip, limit=limit, get_total=True)
        return job_list

    def fetch_job_detail(self, id: str, param: str = None):
        job_details = self.mongodb.fetch_entries(collection_name='jobs', filters={'_id': id, 'is_active': True})
        job_details = job_details['data']
        if param is None:
            return job_details
        if len(job_details) == 0:
            return job_details
        return job_details[0][param]

    def create_job(self, data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            # username = data['username']
            product = data['product'].replace(' ', '_')
            bucket_type = data['bucket_type'].replace(' ', '_')
            sales_order_no = int(data['sales_order_no'])
            customer_portfolio = CustomerPortfolio()
            customer_id = int(CustomerDetailsConfig.CUSTOMER_ID)
            batch_no = customer_portfolio.fetch_batch_no(query_params={'customer_name': CustomerDetailsConfig.CUSTOMER_NAME, 'customer_id': customer_id})
            file_name = data['file_name']
            today_date = datetime.today().strftime("%d_%m_%Y")
            job_name = f'{product}_{bucket_type}_Cycle_{sales_order_no}_{today_date}'
            job_id = str(uuid4())
            file_size = convert_bytes(os.path.getsize(f'{current_app.config[ALLOCATION_FOLDER]}/{file_name}'))
            job = JobModel(
                _id=job_id,
                name=job_name,
                file_name=file_name,
                file_size=file_size,
                product_type=data['product'],
                bucket_type=data['bucket_type'],
                batch_no=batch_no,
                sales_order_no=sales_order_no,
                customer_id=customer_id,
                customer_name=CustomerDetailsConfig.CUSTOMER_NAME,
                created_date=datetime.today(),
                modified_date=datetime.today()
                # created_by=username,
                # modified_by=username
            )
            data = [asdict(job)]
            is_job_created = self.mongodb.create_entries(collection_name='jobs', data=data)
            return {'job_id': job_id, 'is_job_created': is_job_created}
        except KeyError as e:
            return {'is_job_created': False, 'errors': {'message': 'Insufficient request parameters.', 'params': e.args}}

    def update_job(self, id: str, update_params: Dict[str, Any]):
        is_updated = self.mongodb.update_one(collection_name='jobs', filter={'_id': id}, update_params=update_params)
        return is_updated
