"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
import os
import sys
import time
from typing import Dict, Any, List
from uuid import uuid4

import numpy as np
from flask import current_app
from pandas import DataFrame

from actions.RecipeActions import RecipeActions
from constants import MASKED_STRING_LENGTH, RESERVED_KEYWORDS, RESERVED_COLUMN_NAMES, FRESH_REPEAT_STATUS_COL, \
    BOUNCE_RATE_COL, REPEAT_FREQUENCY_COL
from data_cleaning.data_validation import validate_columns
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.CustomErrors import InvalidInputError
from utils.classes.Job import Job
from utils.functions import read_file
from utils.functions.helper import remove_system_columns_from_headers
from utils.functions.historical_data_treatment import last_three_month_data_load
from utils.functions.operations import create_action
from utils.functions.operations import map_columns, create_column_uuid_mapping


def process_file(job_id: str, header_mapping_datatype: Dict[str, str]) -> Dict[str, Any]:
    start_time = time.time()
    mongodb = MongoDB()
    job = Job(mongodb=mongodb)
    try:
        file_name = job.fetch_job_detail(id=job_id, param='file_name')
        df = read_file(file_name=file_name)
        row_count, col_count = df.shape
        df = remove_blanks_and_empty_rows(df=df)
        # Compute L3M data for 'Fresh', 'Repeat & Cleared' and 'Repeat & Bounced'
        df = last_three_month_data_load(df=df, header_mapping_datatype=header_mapping_datatype)
        header_mapping_datatype = {**header_mapping_datatype, FRESH_REPEAT_STATUS_COL: 'Derived Status', BOUNCE_RATE_COL: 'Number', REPEAT_FREQUENCY_COL: 'Number'}
        # Call functions to validate headers and subsequently validate data and dump data into MongoDB
        job_ref_id = str(uuid4())
        data, invalid_counts = validate_columns(df, header_mapping_datatype)
        column_mapping, rev_column_mapping = create_column_uuid_mapping(data[0])
        updated_data = map_columns(data, rev_column_mapping)
        redis = Redis(db=1)
        redis.create_entries(id=job_id, key='jobs', data=job_ref_id)
        action_details = create_action(action='Raw Data')
        recipe = RecipeActions(job_id=job_id)
        recipe.add(action_details=action_details, new_job_ref_id=job_ref_id, column_mapping=column_mapping,
                   rev_column_mapping=rev_column_mapping)
        mongodb.create_entries(collection_name=job_id, data=updated_data)
        headers = compute_column_headers(data)
        job.update_job(id=job_id, update_params={'row_count': row_count, 'headers': headers,
                                                 'header_mapping_datatype': header_mapping_datatype,
                                                 'invalid_counts': invalid_counts, 'is_header_validated': True})
        return {'is_entry_created': True}
    except TypeError as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f'Process File Function: Job ID: {job_id} --> Error: {e} -> Stack Trace: {exc_type, fname, exc_tb.tb_lineno, e}')
        return {'errors': {'message': 'Type Error', 'args': e.args}, 'is_entry_created': False}
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(f'Process File Function: Job ID: {job_id} --> Error: {e} -> Stack Trace: {exc_type, fname, exc_tb.tb_lineno, e}')
        return {'errors': {'message': 'Base Error', 'args': e.args}, 'is_entry_created': False}


def mask_values(value, count):
    count = int(count)
    value = str(value)
    number_of_x = current_app.config[MASKED_STRING_LENGTH] - (abs(count))
    if len(value) <= abs(count):
        return value
    if count < 0:
        return number_of_x * 'X' + value[count:]
    else:
        return value[:count] + number_of_x * 'X'


def remove_blanks_and_empty_rows(df: DataFrame) -> DataFrame:
    # Using regex to remove all instances of blanks
    df = df.replace({"^\s*|\s*$": ""}, regex=True)
    # If there remained only empty string "", change to NaN
    df = df.replace({"": np.nan})
    df.dropna(how='all', inplace=True)
    df = df.replace({np.nan: None})
    return df


def compute_column_headers(data: List[Dict[str, Any]], job_id=None) -> List[str]:
    """
    As it is the structure of the database is an object within an object,
    it is not possible to construct a proper DataFrame.
    Hence, with that in mind, only the first row is taken into account
    to compute the column headers as the data from the database was initially constructed
    using a DataFrame, and hence, the columns for all the rows will be identical.
    """
    column_headers = []
    if len(data) > 0:
        try:
            column_headers = list(data[0].keys())
        except ValueError as e:
            logging.error(f'Compute Column Headers Function: Job ID: {job_id} --> Error: {e}')
    else:
        job = Job()
        column_headers = job.fetch_job_detail(id=job_id, param='headers')
    """
    VALID_STATUS_COL and '_id' keys are not needed to be shown to the UI.
    So, removing them from the list.
    """
    column_headers = remove_system_columns_from_headers(headers=column_headers)
    return column_headers


def map_column_headers(df: DataFrame, columns: Dict[str, str]) -> DataFrame:
    df.rename(columns=columns, inplace=True)
    df = df.replace({np.nan: None})
    return df


def verify_keywords(columns: List[str]):
    """
      - Certain columns are created by the system -> so checking if those exact columns are not
        provided by the user.
      - In every system_created_column, the last string separated by '_' is the served keyword.
        Hence, if that keyword in that position for a particular column, error is thrown.
    """
    reserved_column_list = []
    for column in columns:
        if column in RESERVED_COLUMN_NAMES:
            reserved_column_list.append(column)
        else:
            values = column.split('_')
            if values[-1] in RESERVED_KEYWORDS:
                reserved_column_list.append(column)
    if len(reserved_column_list) > 0:
        # Writing error_code here as could not access it from within InvalidInputError class.
        raise InvalidInputError(f'System reserved keywords have been used. Invalid columns: {", ".join(map(str, reserved_column_list))}')
    return
