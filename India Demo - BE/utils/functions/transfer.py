"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Dict, Any

from sqlalchemy import create_engine

from actions.RecipeActions import RecipeActions
from config.config import ENGAGE_DBConfig
from constants import ENGAGE_TABLE_COLUMNS, VALID_STATUS_COL, REV_COLUMN_MAPPING_COL, COL_MAPPING_COL, \
    ENGAGE_LOAN_ACC_COL, ENGAGE_TABLE_NAME
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.Job import Job
from utils.classes.Mapping import Mapping
from utils.functions.analytics import map_column_headers
from utils.functions.file import parse_records
from utils.functions.jobs import fetch_current_job_ref_id
from utils.functions.operations import map_columns


def process_engage_data(job_id: str, table_name: str, user_mapping: Dict[str, str] = None) -> tuple[bool, Dict[str, str]]:
    job = Job()
    job_details = job.fetch_job_detail(id=job_id)
    sales_order_no = job_details[0]['sales_order_no']
    if user_mapping is None:
        user_mapping = job_details[0]['user_mapping']

    engage_mapping = dict()
    for k, v in user_mapping.items():
        try:
            engage_mapping[k] = ENGAGE_TABLE_COLUMNS[v]
        except KeyError as e:
            engage_mapping[k] = v
    redis = Redis(db=1)
    current_job_ref_id = fetch_current_job_ref_id(redis=redis, job_id=job_id)
    recipe = RecipeActions(job_id=job_id)
    recipe_results = recipe.fetch_by_reference_id(job_reference_id=current_job_ref_id)
    is_valid_col_name = recipe_results[REV_COLUMN_MAPPING_COL][VALID_STATUS_COL]
    mongodb = MongoDB()
    data = mongodb.fetch_entries(collection_name=job_id, filters={is_valid_col_name: True})
    data = map_columns(data['data'], recipe_results[COL_MAPPING_COL])

    df = parse_records(data=data)
    df['sales_order_no'] = sales_order_no
    df['customerid'] = '835'
    df['job_id'] = job_id
    """
    - These columns are added as they are default columns needed by Engage and if not set in code, they give the
      following error: Error: (pymysql.err.OperationalError) (1364, "Field 'is_published' doesn't have a default value")
    """
    df['is_published'] = 1
    df['points'] = 1

    df = map_column_headers(df=df, columns=engage_mapping)
    mapping = Mapping()
    mapping.create_mapping(job_id=job_id, mapping=user_mapping)
    """
     - Move data to Engage
     - Replacing all empty email cell values with the value {row['email']} = f'{row['sp_account_number']}@noemail.c'
    """
    engage_columns = list(engage_mapping.values()) + ['job_id', 'customerid', 'sales_order_no', 'is_published', 'points']
    df = df[engage_columns]
    for index, row in df.iterrows():
        if 'email' in row and row['email'] is None:
            row['email'] = f'{row[ENGAGE_LOAN_ACC_COL]}@noemail.c'
    try:
        engine = create_engine(
            f'mysql+{ENGAGE_DBConfig.DRIVER}://{ENGAGE_DBConfig.USERNAME}:{ENGAGE_DBConfig.PASSWORD}@{ENGAGE_DBConfig.HOST}:{ENGAGE_DBConfig.PORT}/{ENGAGE_DBConfig.DB_NAME}')
        df.to_sql(con=engine, name=table_name, if_exists='append', index=False)
        job.update_job(id=job_id, update_params={'is_data_in_engage': True})
        return True, engage_mapping
    except Exception as e:
        logging.error(f'Move to Engage Function: Job ID: {job_id} --> Error: {e}')
        return False, engage_mapping


def move_file_to_engage(job_id: str, options: Dict[str, Any]) -> bool:
    user_mapping = options['mappings']
    campaign_dates = options['campaign_dates']
    campaign_start_date = datetime.strptime(campaign_dates[0], '%d-%m-%Y')
    campaign_end_date = datetime.strptime(campaign_dates[1], '%d-%m-%Y')

    recipe = RecipeActions(job_id=job_id)
    recipe.finalize()
    success, engage_mapping = process_engage_data(job_id, table_name=ENGAGE_TABLE_NAME, user_mapping=user_mapping)
    if success is False:
        return success

    job = Job()
    job.update_job(id=job_id, update_params={'engage_mapping': engage_mapping, 'user_mapping': user_mapping,
                                             'modified_date': datetime.today(),
                                             'campaign_start_date': campaign_start_date,
                                             'campaign_end_date': campaign_end_date, 'campaign_dates': campaign_dates})

    return True
