"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from typing import Any, Dict, Tuple, List
from uuid import uuid4

from pandas import DataFrame

from actions.RecipeActions import RecipeActions
from constants import MENU_OPTIONS_ACTION_DETAILS, \
    MENU_OPTIONS_ADD_DYNAMIC_MAPPING, MENU_OPTIONS_ADD_STATIC_VALUE, MENU_OPTIONS_CALCULATE_WAIVER, \
    MENU_OPTIONS_DELETE_COLUMN, MENU_OPTIONS_MASK, MENU_OPTIONS_MERGE, MENU_OPTIONS_RANGE, MENU_OPTIONS_RATIO, \
    MENU_OPTIONS_RENAME, MENU_OPTIONS_SPLIT, MENU_OPTIONS_STATE_MAPPING, REV_COLUMN_MAPPING_COL, COL_MAPPING_COL, \
    DERIVED_VALID_EMAIL_COL, MENU_OPTIONS_ROUND, MENU_OPTIONS_NUMBER_OF_YEARS
from data_cleaning.data_validation import validate_columns, identify_header_datatype, create_custom_spacy_entity_ruler
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.Job import Job
from utils.classes.LinkedList import LinkedList
from utils.functions import common_between_lists, remove_duplicates_from_list, get_string_pairs_from_dict
from utils.functions.helper import remove_system_columns_from_headers
from utils.functions.jobs import fetch_current_job_ref_id


def compute_undo_redo(job_id: str, mongodb: MongoDB, redis: Redis) -> Tuple[bool, bool]:
    job_ref_id = fetch_current_job_ref_id(redis=redis, job_id=job_id)
    linked_list = LinkedList(job_id=job_id, mongodb=mongodb)
    current_node = linked_list.fetch_current_node(job_ref_id=job_ref_id)
    is_undo_possible = False
    is_redo_possible = False
    try:
        is_undo_possible = bool(current_node['prev_node'])
        is_redo_possible = bool(current_node['next_node'])
    except TypeError as e:
        pass
    return is_undo_possible, is_redo_possible


def create_action(action: str, args: Tuple = None) -> Dict[str, Any]:
    description = ''
    action_details = {'action': action}
    if action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_SPLIT]:
        col, new_col1, new_col2, separator = args
        description = f'Column "{col}" was split into "{new_col1}" and "{new_col2}" using the separator "{separator}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_MERGE]:
        col1, col2, new_col, separator = args
        description = f'Columns "{col1}" and "{col2}" were merged into "{new_col}" using the separator "{separator}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_DELETE_COLUMN]:
        col = args
        description = f'Column "{col}" was deleted.'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_RENAME]:
        prev_col, new_col = args
        description = f'Column "{prev_col}" was renamed to "{new_col}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_CALCULATE_WAIVER]:
        col_name, percentage = args
        description = f'Column "{col_name}_post_waiver" was created post {percentage}% waiver calculation on "{col_name}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_MASK]:
        column_list, no_of_unmasked_char = args
        description = f'Columns "{column_list}" were masked with {no_of_unmasked_char} characters left visible.'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_RANGE]:
        col_name, conditions = args
        description = f'Column "{col_name}_range" was created for {conditions} applied on "{col_name}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_RATIO]:
        col1, col2 = args
        description = f'Column "{col1}_{col2}_ratioed" was created for ratio of "{col1}" and "{col2}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_STATE_MAPPING]:
        col = args
        description = f'Column "states" was created by mapping state codes from "{col}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_ROUND]:
        col, op = args
        description = f'Column "{col}_{op}" was created by performing "{op}" on "{col}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_ADD_STATIC_VALUE]:
        new_col, value = args
        description = f'Column "{new_col}" was created with static value - "{value}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_ADD_DYNAMIC_MAPPING]:
        col_name, new_col, values = args
        value_args = ''
        for value in values:
            value_args += f'{get_string_pairs_from_dict(value=value)}, '
        value_args = value_args[:len(value_args)-2]
        description = f'Column "{new_col}" was created with dynamic values - "{value_args}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_STATE_MAPPING]:
        col = args
        description = f'Column "states" was created by mapping state codes from "{col}".'
    elif action == MENU_OPTIONS_ACTION_DETAILS[MENU_OPTIONS_NUMBER_OF_YEARS]:
        col = args
        description = f'Column "states" was created by mapping state codes from "{col}".'
    action_details['description'] = description
    return action_details


# deprecate
def process_operations(df: DataFrame, job_id: str, current_job_ref_id: str, redis: Redis, mongodb: MongoDB,
                       action_details: Dict[str, Any]):
    new_job_ref_id = str(uuid4())
    header_mapping_datatype = identify_header_datatype(df=df.head(10))
    data, invalid_counts = validate_columns(df=df, col_mapping=header_mapping_datatype)
    is_data_created = mongodb.create_entries(collection_name=job_id, data=data)
    recipe = RecipeActions(job_id=job_id)
    is_recipe_created = recipe.add(action_details=action_details, current_job_ref_id=current_job_ref_id,
                                   new_job_ref_id=new_job_ref_id)
    if is_data_created and is_recipe_created:
        redis.create_entries(id=job_id, key='jobs', data=new_job_ref_id)
        job = Job(mongodb=mongodb)
        job.update_job(id=job_id, update_params={'invalid_counts': invalid_counts,
                                                 'header_mapping_datatype': header_mapping_datatype})
        is_undo_possible, is_redo_possible = compute_undo_redo(job_id=job_id, mongodb=mongodb, redis=redis)
        return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
    return {'data': [], 'errors': 'Could not create new job reference id.'}


def map_columns(data: List[Dict[str, Any]], mapping: Dict[str, str]) -> List[Dict[str, Any]]:
    updated_data = []
    for data_point in data:
        data_keys = list(data_point.keys())
        mapping_keys = list(mapping.keys())
        common_elements = common_between_lists(data_keys, mapping_keys)
        common_elements = remove_duplicates_from_list(common_elements)
        updated_data_point = {}
        for col_old_name, col_new_name in mapping.items():
            updated_data_point[col_new_name] = data_point[col_old_name]
        updated_data.append(updated_data_point)
    return updated_data


def create_column_uuid_mapping(dataItem: Dict[str, Any]) -> Tuple[Dict[str, str], Dict[str, str]]:
    column_mapping = {}
    rev_column_mapping = {}
    for key in dataItem.keys():
        uuid = str(uuid4())
        if key == '_id':
            uuid = '_id'
        column_mapping[uuid] = key
        rev_column_mapping[key] = uuid
    return column_mapping, rev_column_mapping


def process_operations_v2(job_id: str, current_job_ref_id: str, redis: Redis, mongodb: MongoDB,
                          action_details: Dict[str, Any], df: DataFrame = None, delete_column: str = '',
                          rename_column: Dict[str, str] = None, user_defined_datatype: Dict[str, str] = None):
    new_job_ref_id = str(uuid4())
    col_mapping = {}
    rev_col_mapping = {}
    invalid_counts = {}
    header_mapping_datatype = {}
    job = Job(mongodb=mongodb)
    old_header_mapping_datatype = job.fetch_job_detail(id=job_id, param='header_mapping_datatype')
    if df is not None:
        header_mapping_datatype = identify_header_datatype(df=df.head(10))
        if user_defined_datatype is not None:
            header_mapping_datatype = {**header_mapping_datatype, **user_defined_datatype}
        data, invalid_counts = validate_columns(df=df, col_mapping=header_mapping_datatype, add_meta=False)
        if DERIVED_VALID_EMAIL_COL in data[0]:
            del data[0][DERIVED_VALID_EMAIL_COL]
        col_mapping, rev_col_mapping = create_column_uuid_mapping(data[0])
        data = map_columns(data, rev_col_mapping)
        is_data_created = mongodb.upsert_many(collection_name=job_id, docs=data)
        if is_data_created is False:
            return {'data': [], 'errors': 'Could not update data'}

    recipe = RecipeActions(job_id=job_id)
    curr_recipe = recipe.fetch_by_reference_id(current_job_ref_id)

    if delete_column != '':
        for k, v in curr_recipe[COL_MAPPING_COL].items():
            if v == delete_column:
                del curr_recipe[COL_MAPPING_COL][k]
                del curr_recipe[REV_COLUMN_MAPPING_COL][v]
                break

    if rename_column is not None:
        for k, v in curr_recipe[COL_MAPPING_COL].items():
            for old_name, new_name in rename_column.items():
                if v == old_name:
                    header_mapping_datatype[new_name] = old_header_mapping_datatype[old_name]
                    curr_recipe[COL_MAPPING_COL][k] = new_name
                    del curr_recipe[REV_COLUMN_MAPPING_COL][v]
                    curr_recipe[REV_COLUMN_MAPPING_COL][new_name] = k
                    break

    final_col_mapping = {**curr_recipe[COL_MAPPING_COL], **col_mapping}
    final_rev_col_mapping = {**curr_recipe[REV_COLUMN_MAPPING_COL], **rev_col_mapping}

    is_recipe_created = recipe.add(action_details=action_details, current_job_ref_id=current_job_ref_id, new_job_ref_id=new_job_ref_id, column_mapping=final_col_mapping, rev_column_mapping=final_rev_col_mapping)

    if is_recipe_created:
        redis.create_entries(id=job_id, key='jobs', data=new_job_ref_id)
        if df is not None or rename_column is not None:
            header_mapping_datatype = {**old_header_mapping_datatype, **header_mapping_datatype}
            updated_header = list(header_mapping_datatype.keys())
            updated_header = remove_system_columns_from_headers(headers=updated_header)
            update_params = {'header_mapping_datatype': header_mapping_datatype, 'headers': updated_header}
            if df is not None:
                update_params['invalid_counts'] = invalid_counts
            job.update_job(id=job_id, update_params=update_params)
        is_undo_possible, is_redo_possible = compute_undo_redo(job_id=job_id, mongodb=mongodb, redis=redis)
        return {'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
    return {'data': [], 'errors': 'Could not create new job reference id.'}
