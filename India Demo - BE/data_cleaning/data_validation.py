"""
Author: Viral Mamniya
Author E-mail: viral.mamniya@spocto.com
"""

from __future__ import annotations

import json
import logging
import os
import sys
from typing import Any, Dict, List, Tuple
from uuid import uuid4

import numpy as np
import pandas as pd
from pandas import DataFrame
from spacy.lang.en import English

from constants import DATATYPE_MAPPING, DTYPE_MAPPING, FUNCTION_MAPPING, DERIVED_VALID_EMAIL_COL, \
    DERIVED_VALID_MOBILE_COL, VALID_STATUS_COL
from data_cleaning import validate_records


def identify_header_datatype(df: DataFrame) -> Dict[str, str]:
    columns = df.columns
    header_cols = {}
    with open('historical_column_mapping.json', 'r') as historical_file:
        historical_data = json.load(historical_file)
    for column in columns:
        is_added = False
        for key, value in historical_data.items():
            if is_added:
                continue
            col_name = column.lower().strip()
            if col_name in value:
                if key in DATATYPE_MAPPING.keys():
                    header_cols[column] = DATATYPE_MAPPING.get(key)
                    is_added = True
            else:
                data_type_dict = df[column].dtype
                if 'mobile' in col_name:
                    header_cols[column] = 'Mobile'
                elif 'email' in col_name:
                    header_cols[column] = 'Email'
                elif DTYPE_MAPPING[str(data_type_dict)] is not None:
                    header_cols[column] = DTYPE_MAPPING[str(data_type_dict)]
                else:
                    header_cols[column] = 'Text'
    header_cols[DERIVED_VALID_MOBILE_COL] = DATATYPE_MAPPING.get('mobile')
    header_cols[DERIVED_VALID_EMAIL_COL] = DATATYPE_MAPPING.get('email')
    return header_cols


# function to update invalid count in object. 
def invalid_cols_count(key, final_val, final_count_obj):
    if isinstance(final_val, dict):
        if final_val[VALID_STATUS_COL] is False:
            if key in final_count_obj:
                final_count_obj[key] = final_count_obj.get(key, 0) + 1
            else:
                final_count_obj[key] = 1
        else:
            if key not in final_count_obj:
                final_count_obj[key] = 0
    else:
        for i in range(len(final_val)):
            if final_val[i][VALID_STATUS_COL] is False:
                if key in final_count_obj:
                    final_count_obj[key] = final_count_obj.get(key, 0) + 1
                else:
                    final_count_obj[key] = 1
            else:
                if key not in final_count_obj:
                    final_count_obj[key] = 0
    return final_count_obj


def create_custom_spacy_entity_ruler() -> English:
    nlp = English()
    ruler = nlp.add_pipe('entity_ruler')
    with open('data_cleaning/location_ner/location.json', 'r', encoding='utf8') as location_file:
        patterns = json.load(location_file)
    ruler.add_patterns(patterns['locations'])
    paths = 'data_cleaning/location_ner'
    nlp.to_disk(paths)
    nlp.from_disk(paths)
    return nlp


def validate_columns(df: DataFrame, col_mapping: Dict[str, str], add_meta: bool = True) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    try:
        # Converting DataFrame to Dict.
        data = df.to_dict(orient='records')
        final_obj = []
        final_count_obj = {}
        # Iterating over dictionary and based on header data-type calling specific functions for cleaning and validation.
        required_key = ['Account Number', 'Mobile']
        for row in data:
            obj = {}
            final_val = None
            valid_mobile = None
            for key, cell in row.items():
                key_for_function = col_mapping[key]
                function_to_call = FUNCTION_MAPPING[key_for_function]
                if not pd.isna(cell):
                    final_val = getattr(validate_records, function_to_call)(str(cell))
                    if isinstance(final_val, dict):
                        obj[key] = final_val
                        final_count_obj = invalid_cols_count(key, final_val, final_count_obj)
                        if function_to_call == 'check_email':
                            if final_val[VALID_STATUS_COL]:
                                obj[DERIVED_VALID_EMAIL_COL] = final_val
                            else:
                                obj[DERIVED_VALID_EMAIL_COL] = {'value': None, VALID_STATUS_COL: False}
                    else:
                        # Mobile output returned as a list of dictionaries.
                        for i in range(len(final_val)):
                            if len(final_val) > 1:
                                key_name = f'{key}_{i}'
                            else:
                                key_name = key
                            obj[key_name] = final_val[i]
                            final_count_obj = invalid_cols_count(key_name, final_val, final_count_obj)
                            if valid_mobile is None:
                                valid_mobile = final_val[i]
                else:
                    obj[key] = {'value': None, VALID_STATUS_COL: False}
                    if function_to_call == 'check_email':
                        obj[DERIVED_VALID_EMAIL_COL] = {'value': None, VALID_STATUS_COL: False}
                    final_count_obj = invalid_cols_count(key, final_val, final_count_obj)
            # Flagging the entire row as valid or invalid based on DERIVED_VALID_MOBILE
            if add_meta:
                obj[VALID_STATUS_COL] = valid_mobile is not None
                # Adding unique UUID against each record.
                obj['_id'] = str(uuid4())
                obj[DERIVED_VALID_MOBILE_COL] = valid_mobile if valid_mobile is not None else {'value': None, VALID_STATUS_COL: False}
            else:
                obj['_id'] = row['_id']
            final_obj.append(obj)
        return final_obj, final_count_obj
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        logging.error(exc_type, fname, exc_tb.tb_lineno, e)
