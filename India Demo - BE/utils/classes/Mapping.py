"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, Tuple, List
from uuid import uuid4

from models.Mapping import Mapping as MappingModel
from schemas.MongoDB import MongoDB
from utils.classes.Job import Job


class Mapping:
    def __init__(self, mongodb: MongoDB = None):
        if mongodb is None:
            self.mongodb = MongoDB()
        else:
            self.mongodb = mongodb
        self.collection_name = 'mappings'

    def fetch_mapping_details(self, job_id: str, param: str = None) -> Tuple[Dict[str, str], List[str]]:
        job = Job(mongodb=self.mongodb)
        job_details = job.fetch_job_detail(id=job_id)
        product_type = job_details[0]['product_type']
        bucket_type = job_details[0]['bucket_type']
        headers = job_details[0]['headers']
        mapping_details = self.mongodb.fetch_entries(collection_name=self.collection_name, filters={'product_type': product_type, 'bucket_type': bucket_type, 'operation': 'sort', 'col_name': 'created_date'})
        mapping_details = mapping_details['data']
        if param is None or len(mapping_details) == 0:
            return mapping_details, headers
        return mapping_details[0][param], headers

    def format_mapping(self, job_id: str) -> Dict[str, Any]:
        """
        The mapping is being formatted in this way to facilitate the auto-mapping
        feature on the front-end.
        The key "label" is the "Label" for the column on the front-end.
        The key "disabled" means that the user will not be allowed to select this option again.
        """
        mapping_details, _ = self.fetch_mapping_details(job_id=job_id, param='mapping')
        formatted_mapping = {}
        for k, v in mapping_details.items():
            formatted_mapping[k] = {'label': v, 'disabled': False}
        return formatted_mapping

    def create_mapping(self, job_id: str, mapping: Dict[str, str]) -> Dict[str, Any]:
        try:
            job = Job(mongodb=self.mongodb)
            job_details = job.fetch_job_detail(id=job_id)
            product_type = job_details[0]['product_type']
            bucket_type = job_details[0]['bucket_type']
            mapping_id = str(uuid4())
            engage_mapping = MappingModel(
                _id=mapping_id,
                job_id=job_id,
                product_type=product_type,
                bucket_type=bucket_type,
                mapping=mapping,
                created_date=datetime.today()
            )
            data = [asdict(engage_mapping)]
            is_mapping_created = self.mongodb.create_entries(collection_name=self.collection_name, data=data)
            return {'is_mapping_created': is_mapping_created}
        except KeyError as e:
            logging.error(f'Create Mapping Function: Job ID: {job_id} --> Error: {e}')
            return {'is_mapping_created': False, 'errors': {'message': 'Insufficient request parameters.', 'params': e.args}}

    def update_mapping(self, id: str, update_params: Dict[str, Any]):
        return self.mongodb.update_one(collection_name=self.collection_name, filter={'_id': id}, update_params=update_params)
