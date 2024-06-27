"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import chardet
import logging
import os
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any, Tuple
from uuid import uuid4

import numpy as np
import pandas as pd
from flask import current_app
from flask import request
from pandas import DataFrame
from werkzeug.utils import secure_filename

from actions.Filters import Filters
from constants import INVALID_FOLDER, ALLOCATION_FOLDER, PAYMENT_FOLDER, EXCEL_ALLOWED_TYPES, CSV_ALLOWED_TYPES, \
    VALID_STATUS_COL
from schemas.MongoDB import MongoDB
from utils.functions import allowed_file
from utils.functions.analytics import verify_keywords, remove_blanks_and_empty_rows


def create_file(job_id: str, type: str = None, options: Dict[str, str] = None) -> str | None:
    folder_type = ALLOCATION_FOLDER
    mongodb = MongoDB()
    job_details = mongodb.fetch_entries(collection_name='jobs', filters={'_id': job_id})
    file_name = job_details['data'][0]['file_name']
    file_name, extension = file_name.split('.')
    file_name += '.csv'
    if options is None:
        options = {'name': 'test', 'format': 'CSV', 'compressed': False, 'encoding': 'utf-8'}
    action = Filters(job_id=job_id)
    if type == 'invalid':
        data = action.invalid()
        folder_type = INVALID_FOLDER
        file_name = f'INVALID_{file_name}'
    else:
        data = action.all()
    args = {'encoding': options['encoding'], 'index': False}
    """
    The snippet below needs to be tested for options['compressed']:
    if options['compressed']:
        file_name += '.bz2'
        args['compression'] = 'bz2'
    """
    df = parse_records(data=data['data'])
    filepath = Path(f"{current_app.config[folder_type]}/{file_name}")
    filepath.parent.mkdir(parents=True, exist_ok=True)
    try:
        if options['format'] == 'CSV':
            df.to_csv(filepath, **args)
        elif options['format'] == 'Excel':
            df.to_excel(filepath, **args)
        return file_name
    except Exception as e:
        """
        Implement error logging here
        """
        logging.error(f'Create File Function: Job ID: {job_id} --> Error: {e}')
        return None


def file_preview(file_name: str, folder_type: str) -> Tuple[List[Dict[str, Any]], List[Any], str]:
    extension = Path(file_name).suffix
    filepath = f'{current_app.config[folder_type]}/{file_name}'
    new_file_name = file_name.split('.')[0]
    new_file_name = f'{new_file_name}.pkl'
    df = None
    try:
        if extension in CSV_ALLOWED_TYPES:
            df = pd.read_csv(filepath, dtype=str)
        elif extension in EXCEL_ALLOWED_TYPES:
            df = pd.read_excel(filepath, dtype=str)
    except pd.errors.EmptyDataError as e:
        logging.error(f'File completely empty -> File Name: {file_name} -> Error: {e}')
    except UnicodeDecodeError as e:
        with open(filepath, 'rb') as file:
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
        df = pd.read_csv(filepath, encoding=encoding)
    data, columns = convert_parse_df_to_dict_preview(df=df, folder_type=folder_type, file_name=new_file_name)
    return data, columns, new_file_name


def parse_records(data: List[Dict[str, Any]], skip_cols: List[str] = None) -> DataFrame:
    """
    As the database schema is literally Object within an Object,
    and it cannot be exported into a DataFrame and subsequently into
    a CSV file, this function removes the innermost object and makes
    it a flat object (i.e. Dict[str, str]).
    """
    if skip_cols is None:
        skip_cols = ['_id', VALID_STATUS_COL, 'job_ref_id']
    final_data = []
    for row in data:
        new_row = {}
        for k, v in row.items():
            if k not in skip_cols:
                if isinstance(v, dict):
                    new_row[k] = v['value']
                else:
                    new_row[k] = v
        final_data.append(new_row)
    df = pd.DataFrame(final_data)
    return df


def verify_and_save_uploaded_file(file_request, folder_type: str):
    # Check if the POST request has the file part
    if 'file' not in file_request:
        return 'User failed to upload a file. File name is empty.'
    file = file_request['file']
    # If the user does not select a file, the browser submits an empty file without a filename.
    if file.filename == '':
        return 'User failed to upload a file. File name is empty.'
    if file and allowed_file(file.filename):
        today_date = datetime.today().strftime("%d-%m-%Y_%H-%M-%S")
        file_name = secure_filename(file.filename)
        file_name, extension = file_name.split('.')
        file_name = f'{file_name}_{today_date}.{extension}'
        file.save(os.path.join(current_app.config[folder_type], file_name))
        return file_name


def save_file():
    FOLDER_TYPE = ALLOCATION_FOLDER
    if 'payments' in request.url:
        FOLDER_TYPE = PAYMENT_FOLDER
    file_name = verify_and_save_uploaded_file(request.files, folder_type=FOLDER_TYPE)
    return file_name


def convert_parse_df_to_dict_preview(df: DataFrame, folder_type: str, file_name: str) -> Tuple[List[Dict[str, Any]], List[Any]]:
    data = []
    columns = []
    if df is None:
        return data, columns
    df.to_pickle(f'{current_app.config[folder_type]}/{file_name}')
    df = df.head(10)
    columns = list(df.columns)
    df = remove_blanks_and_empty_rows(df=df)
    verify_keywords(columns=columns)
    df['_id'] = df.apply(lambda x: str(uuid4()), axis=1)
    df.replace([np.inf, -np.inf], np.nan, inplace=True)
    df.replace({np.nan: None}, inplace=True)
    data = df.to_dict(orient='records')
    return data, columns
