"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from datetime import date, datetime
from json import JSONDecoder, JSONEncoder
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd
from flask import current_app
from pandas import DataFrame

from constants import ALLOWED_EXTENSIONS, ALLOCATION_FOLDER, TEXT_LIKE_DATATYPES


def read_file(file_name: str, no_of_rows: int = None, folder_type: str = ALLOCATION_FOLDER) -> DataFrame:
    extension = Path(file_name).suffix
    filepath = f'{current_app.config[folder_type]}/{file_name}'
    df = None
    try:
        if extension == '.csv':
            df = pd.read_csv(filepath, nrows=no_of_rows, dtype=str)
        elif extension in ['.xlsx', '.xls']:
            df = pd.read_excel(filepath, nrows=no_of_rows, dtype=str)
        elif extension == '.pkl':
            df = pd.read_pickle(filepath)
    except Exception as e:
        print('Error with dataframe', e)
    return df


def convert_bytes(size_in_bytes: int, to: str = 'MB') -> str:
    CONV_PARAM_KB = 0.001
    CONV_PARAM_MB = 0.000001
    CONV_PARAM_GB = 0.000000001
    size = 1
    if to == 'KB':
        size = size_in_bytes * CONV_PARAM_KB
    elif to == 'MB':
        size = size_in_bytes * CONV_PARAM_MB
    elif to == 'GB':
        size = size_in_bytes * CONV_PARAM_GB
    size = round(size, 3)
    return f'{size} {to}'


def difference_between_lists(array_1: List[Any], array_2: List[Any]) -> List[Any]:
    diff_list = [i for i in array_1 + array_2 if i not in array_1 or i not in array_2]
    return diff_list


def common_between_lists(array_1: List[Any], array_2: List[Any]) -> List[Any]:
    diff_list = [i for i in array_1 + array_2 if i in array_1 and i in array_2]
    return diff_list


def remove_duplicates_from_list(array: List[Any]) -> List[Any]:
    return list(set(array))


def get_dict_keys_from_list(mapping: Dict[str, str], headers: List[str]) -> Dict[str, str]:
    new_user_mapping = {}
    if isinstance(mapping, dict):
        for k, v in mapping.items():
            if k in headers:
                new_user_mapping[k] = v
    return new_user_mapping


def get_string_pairs_from_dict(value: Dict[str, Any]) -> str:
    string_pairs = ''
    for k, v in value.items():
        string_pairs += f'{k} - {v}, '
    string_pairs = string_pairs[:-2]
    return string_pairs


def order_list(array: List[Any], elem_to_order_by: str, elements: List[Any] = None, add_or_order: int = True) -> List[Any]:
    """
      - Case 1: add_or_order = True
        - When a new column is created, and we want to add it to the "array" but in an ordered fashion.
        - Test Case (add_or_order = True)
          - array = ['a', 'b', 'c', 'd']
          - elem_to_order_by = 'c'
          - elements = [1, 2]
          - Result: ['a', 'b', 'c', 1, 2, 'd']
      - Case 2: add_or_order = False
        - When we want to reorder existing elements in the "array".
        - Test Case (add_or_order = False)
          - array = ['a', 'b', 'c', 'd']
          - elem_to_order_by = 'b'
          - elements = ['a', 'c']
          - Result: ['b', 'a', 'c', 'd']
    """
    try:
        if not add_or_order:
            # This logic will find the elements that exist within an array and then position them next to the "elem_to_order_by" element in "array".
            elements_to_be_moved = []
            for value in array:
                for elem in elements:
                    if elem == value:
                        array.remove(elem)
                        elements_to_be_moved.append(elem)
            elements = elements_to_be_moved
        position = array.index(elem_to_order_by)
        return array[:position + 1] + elements + array[position + 1:]
    except ValueError as e:
        """
        This error occurs when the "elem_to_order_by" element does not exist in "array".
        """
        logging.error(f'Order List Method: {e}')
        return array


def split_str_by_pos(string: str, no_of_char: int, orientation: int = 1) -> Tuple[str, str]:
    """
      - orientation == 1 -> Take first <no_of_char> characters
      - orientation == -1 -> Take last <no_of_char> characters
    """
    if orientation == -1:
        split_string = string[len(string)-no_of_char:len(string)]
        remaining_string = string[:len(string)-no_of_char]
    else:
        split_string = string[:no_of_char]
        remaining_string = string[no_of_char:]
    return split_string, remaining_string


def allowed_file(filename):
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS


def isfloat(string: str) -> bool:
    return string.replace('.', '', 1).isdigit()


def get_datatype_appropriate_values(array: List[Any], datatype: str = None) -> List[Any]:
    """
    This function takes in an array of strings, and parses them into the values that are
    actually the datatype they need to be. i.e. '1000' -> 1000 & '1000.75' -> 1000.75.
    For now, it converts from "str" to "int" and "float".
    """
    if datatype in TEXT_LIKE_DATATYPES:
        return array
    parsed_array = []
    for element in array:
        if element.isdigit():
            parsed_array.append(int(element))
        elif isfloat(element):
            parsed_array.append(float(element))
        else:
            parsed_array.append(element)
    return parsed_array


class DateTimeSerializer(JSONEncoder):
    # Overriding the default method
    def default(self, obj):
        if isinstance(obj, (date, datetime)):
            return obj.isoformat()


class DateTimeDeserializer(JSONDecoder):
    # Overriding the default method
    pass
