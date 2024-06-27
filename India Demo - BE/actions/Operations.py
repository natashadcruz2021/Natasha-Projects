"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from typing import Any, Dict

import numpy as np
import pandas as pd

from actions.Filters import Filters
from constants import MENU_OPTIONS_ROUND_OFF, MENU_OPTIONS_ROUND_DOWN, ORIENTATION, STATE, MENU_OPTIONS_NUMBER_OF_YEARS, \
    MENU_OPTIONS_AGE, SYSTEM_DATETIME_FORMAT
from constants.StateMapping import STATE_MAPPING
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.functions import split_str_by_pos
from utils.functions.analytics import mask_values
from utils.functions.file import parse_records
from utils.functions.jobs import fetch_current_job_ref_id
from utils.functions.operations import compute_undo_redo, create_action, process_operations_v2
from datetime import datetime
from dateutil import relativedelta


class Operations:
    def __init__(self, job_id: str):
        self.redis = Redis(db=1)
        self.mongodb = MongoDB()
        self.job_id = job_id
        self.filter_actions = Filters(job_id=self.job_id)

    def split(self, options: Dict[str, str]) -> Dict[Any, Any]:
        if isinstance(options, dict):
            try:
                new_col1 = options['new_col1']
                new_col2 = options['new_col2']
                col = options['col']
                separator = options['separator']
                data = self.filter_actions.all(projection=[col])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                if separator == 'position':
                    no_of_char = int(options['no_of_char'])
                    orientation = ORIENTATION.get(options['orientation'])
                    data = df.to_dict(orient='records')
                    for row in data:
                        row[new_col1], row[new_col2] = split_str_by_pos(string=row[col], no_of_char=no_of_char,
                                                                        orientation=orientation)
                    df = pd.DataFrame(data)
                else:
                    df[[new_col1, new_col2]] = df[col].str.split(separator, n=1, expand=True)
                df = df.replace({np.nan: None})
                action_details = create_action(action='Split', args=(col, new_col1, new_col2, separator))
                df = df[['_id', new_col1, new_col2]]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def merge(self, options: Dict[str, str]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                new_col = options['new_col']
                separator = options['separator']
                col1 = options['col1']
                col2 = options['col2']
                data = self.filter_actions.all(projection=[col1, col2])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                column_datatypes = {'col1': df[col1].dtypes, 'col2': df[col2].dtypes}
                df = df.astype({col1: str, col2: str})
                df[new_col] = df[col1] + separator + df[col2]
                if column_datatypes['col1'] in [int, float]:
                    df = df.astype({col1: column_datatypes['col1']})
                if column_datatypes['col2'] in [int, float]:
                    df = df.astype({col2: column_datatypes['col2']})
                df = df.replace({f'None{separator}None': np.nan})
                df = df.replace({np.nan: None})
                action_details = create_action(action='Merge', args=(col1, col2, new_col, separator))
                df = df[['_id', new_col]]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                print(e)
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def delete_column(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                col = options['col']
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                action_details = create_action(action='Delete Column', args=col)
                process_operations_v2(delete_column=col, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def rename(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                prev_col = options['prev_col']
                new_col = options['new_col']
                action_details = create_action(action='Rename', args=(prev_col, new_col))
                process_operations_v2(rename_column={prev_col: new_col}, job_id=self.job_id,
                                      current_job_ref_id=current_job_ref_id, mongodb=self.mongodb,
                                      redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def calculate_value_post_waiver(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                col_name = options['col_name']
                percentage = options['percentage']
                data = self.filter_actions.all(projection=[col_name])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                df[f'{col_name}_post_waiver_{percentage}_percent'] = df[col_name] * (1 - (percentage / 100))
                df[f'{col_name}_post_waiver_{percentage}_percent'] = df[
                    f'{col_name}_post_waiver_{percentage}_percent'].round(decimals=2)
                df = df.replace({np.nan: None})
                action_details = create_action(action='Waiver Value Calculation', args=(col_name, percentage))
                df = df[['_id', f'{col_name}_post_waiver_{percentage}_percent']]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def mask(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                columns = options['columns']
                no_of_unmasked_char = options['no_of_unmasked_char']
                data = self.filter_actions.all(projection=[columns])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                for column in columns:
                    df[column] = df[column].apply(mask_values, count=no_of_unmasked_char)
                column_list = ', '.join(columns)
                df = df.replace({np.nan: None, 'nan': None})
                action_details = create_action(action='Mask', args=(column_list, no_of_unmasked_char))
                df = df[columns + ['_id']]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def range(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                range_conditions = options['range_conditions']
                col_name = options['col_name']
                conditions = []
                data = self.filter_actions.all(projection=[col_name])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                df[f'{col_name}_range'] = df[col_name]
                for x in range_conditions:
                    higher_range = df[col_name].max()
                    lower_range = df[col_name].min()
                    if 'higher_range' in x:
                        higher_range = int(x['higher_range'])
                    if 'lower_range' in x:
                        lower_range = int(x['lower_range'])
                    conditions.append(f'{higher_range}-{lower_range}')
                    df[f'{col_name}_range'] = np.where((df[col_name] <= higher_range) & (df[col_name] >= lower_range),
                                                       f'{lower_range}-{higher_range}', df[f'{col_name}_range'])
                condition_list = ', '.join(conditions)
                df = df.replace({np.nan: None})
                action_details = create_action(action='Range', args=(col_name, condition_list))
                df = df[[f'{col_name}_range', '_id']]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                logging.error(f'Error in Range: -> Range Conditions: {range_conditions} -> {e}')
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def ratio(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                col1 = options['col1']
                col2 = options['col2']
                data = self.filter_actions.all(projection=[col1, col2])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                df[f'{col1}_{col2}_ratioed'] = df[col1] / df[col2]
                df.replace([np.inf, -np.inf], np.nan, inplace=True)
                df.replace({np.nan: None}, inplace=True)
                action_details = create_action(action='Ratio', args=(col1, col2))
                df = df[['_id', f'{col1}_{col2}_ratioed']]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
            except Exception as e:
                return {'data': [], 'errors': f'BaseError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def add_column_static_value(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                value = options['value']
                new_col = options['new_col']
                data = self.filter_actions.all(projection=[])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                df[new_col] = value
                df.replace({np.nan: None}, inplace=True)
                action_details = create_action(action='Add Column - Static Value', args=(new_col, value))
                df = df[['_id', new_col]]
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def add_column_dynamic_mapping(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                col_name = options['col_name']
                values = options['values']
                datatype = options['datatype']
                new_col = options['new_col']
                data = self.filter_actions.all(projection=[col_name])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                # If the below line of code is not set, gives a KeyError for the column as it is not yet created.
                df[new_col] = None
                for value in values:
                    for k, v in value.items():
                        df[new_col] = np.where(df[col_name] == k, v, df[new_col])
                df.replace({np.nan: None}, inplace=True)
                action_details = create_action(action='Add Column - Dynamic Mapping', args=(col_name, new_col, values))
                df = df[['_id', new_col]]
                user_defined_datatype = {new_col: datatype}
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                      user_defined_datatype=user_defined_datatype)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def region_mapping(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                col = options['col']
                new_col = col + '_' + STATE
                data = self.filter_actions.all(projection=[col])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                state_code_column = df[col].tolist()
                state_mapped_list = []
                for state_code in state_code_column:
                    state_mapped_list.append(STATE_MAPPING.get(state_code))
                df[new_col] = state_mapped_list
                df.replace({np.nan: None}, inplace=True)
                action_details = create_action(action='State Mapping', args=col)
                user_defined_datatype = {new_col: 'Location'}
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                      user_defined_datatype=user_defined_datatype)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except Exception as e:
                print(e)
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def round_numbers(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                col = options['col']
                op = options['op']
                data = self.filter_actions.all(projection=[col])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                df[col + '_' + op] = df[col].round() if op == MENU_OPTIONS_ROUND_OFF \
                    else df[col].apply(np.floor) if op == MENU_OPTIONS_ROUND_DOWN \
                    else df[col].apply(np.ceil)
                df.replace({np.nan: None}, inplace=True)
                action_details = create_action(action='Rounding Operation', args=(col, op))
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except Exception as e:
                return {'data': [], 'errors': 'Some exception occurred', 'exception': str(e)}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def calculate_number_of_years(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                column = options['col']
                operation = options['op']
                data = self.filter_actions.all(projection=[column])
                current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
                df = parse_records(data['data'], skip_cols=[])
                new_column_name = MENU_OPTIONS_AGE if column == 'DOB' else MENU_OPTIONS_NUMBER_OF_YEARS
                df[new_column_name] = df[column].apply(lambda date: relativedelta.relativedelta(datetime.now(),
                                                                                                datetime.strptime(date, SYSTEM_DATETIME_FORMAT)).years)
                df.replace({np.nan: None}, inplace=True)
                action_details = create_action(action='Number Of Years', args=(column, operation))
                process_operations_v2(df=df, job_id=self.job_id, current_job_ref_id=current_job_ref_id,
                                      mongodb=self.mongodb, redis=self.redis, action_details=action_details)
                is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                                       redis=self.redis)
                return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except Exception as e:
                print(e)
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}
