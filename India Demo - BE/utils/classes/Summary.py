"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from actions.RecipeActions import RecipeActions
from constants import REV_COLUMN_MAPPING_COL, DERIVED_VALID_MOBILE_COL, DERIVED_VALID_EMAIL_COL, VALID_STATUS_COL, \
    SUMMARY_PROPERTY_MAX_MIN, SUMMARY_PROPERTY_MAX, SUMMARY_PROPERTY_MIN
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.Job import Job
from utils.functions.jobs import fetch_current_job_ref_id


class Summary:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.redis = Redis(db=1)
        self.recipe = RecipeActions(job_id=job_id)
        self.mongodb = MongoDB()
        self.current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=job_id)

    def calculate(self) -> Dict[str, Any]:
        try:
            job = Job(mongodb=self.mongodb)
            job_details = job.fetch_job_detail(id=self.job_id)
            job_details = job_details[0]
            current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
            recipe = RecipeActions(self.job_id).fetch_by_reference_id(current_job_ref_id)

            valid_mobile_col = recipe[REV_COLUMN_MAPPING_COL][DERIVED_VALID_MOBILE_COL]
            valid_email_col = recipe[REV_COLUMN_MAPPING_COL][DERIVED_VALID_EMAIL_COL]

            mobile_agg = self.mongodb.aggregate(collection_name=self.job_id, filter={f'{valid_mobile_col}.{VALID_STATUS_COL}': False})
            email_agg = self.mongodb.aggregate(collection_name=self.job_id, filter={f'{valid_email_col}.{VALID_STATUS_COL}': False})
            return {
                'row_count': job_details['row_count'],
                'col_count': recipe['col_count'],
                'total_invalids': {
                    'mobile': mobile_agg,
                    'email': email_agg
                }
            }
        except Exception as e:
            logging.error(f'Compute Job Summary: Job ID: {self.job_id} --> Error: {e}')
            return {'errors': {'args': e.args}}

    def fetch_distinct_values_from_col(self, col_name: str) -> List[Any]:
        current_recipe = self.recipe.fetch_by_reference_id(self.current_job_ref_id)
        db_reversed_col_name = current_recipe[REV_COLUMN_MAPPING_COL][col_name]
        values = self.mongodb.fetch_distinct_entries(collection_name=self.job_id, col_name=db_reversed_col_name)
        return values

    def fetch_max_min_values_from_col(self, col_name: str, type: str = SUMMARY_PROPERTY_MAX_MIN) -> Any:
        current_recipe = self.recipe.fetch_by_reference_id(self.current_job_ref_id)
        db_reversed_col_name = current_recipe[REV_COLUMN_MAPPING_COL][col_name]
        aggregate = self.mongodb.aggregate(collection_name=self.job_id, col_name=db_reversed_col_name, type=SUMMARY_PROPERTY_MAX_MIN)
        if type == SUMMARY_PROPERTY_MAX:
            value = aggregate[0][SUMMARY_PROPERTY_MAX]
        elif type == SUMMARY_PROPERTY_MIN:
            value = aggregate[0][SUMMARY_PROPERTY_MIN]
        else:
            value = aggregate[0]
        return value
