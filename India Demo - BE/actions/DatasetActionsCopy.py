"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
import logging
import numpy as np
import pandas as pd
from typing import Any, Dict
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.LinkedList import LinkedList
from utils.functions.analytics import data_profile, mask_values
from utils.functions import fetch_current_file_ref_id
from utils.functions.operations import compute_profile_undo_redo, create_action, process_operations
from utils.classes.Operation import Operation


class DatasetActions:
    def __init__(self, file_id: str):
        self.redis = Redis(db=1)
        self.mongodb = MongoDB()
        self.file_id = file_id

    def all(self, options: Dict[str, Any] = None) -> Dict[str, Any]:
        limit = 0
        skip = 0
        if options is not None:
            limit = options['limit']
            skip = options['skip']
        current_file_ref_id = fetch_current_file_ref_id(redis=self.redis, file_id=self.file_id)
        results = self.mongodb.fetch_entries(db_name=self.file_id, collection_name=current_file_ref_id, skip=skip, limit=limit, get_total=True)
        is_undo_possible, is_redo_possible, profile = compute_profile_undo_redo(file_id=self.file_id, redis=self.redis, mongodb=self.mongodb)
        return {**results, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}

    def search(self, options: Dict[str, str]) -> Dict[Any, Any]:
        if isinstance(options, dict):
            filters = None
            skip = options['skip']
            limit = options['limit']
            keyword = options['keyword']
            column = options['col_name']
            current_file_ref_id = fetch_current_file_ref_id(redis=self.redis, file_id=self.file_id)
            if column == '':
                # Creating an index to allow for full-text search
                self.mongodb.create_index(db_name=self.file_id, collection_name=current_file_ref_id)
                filters = {'$text': {'$search': keyword}}
            else:
                filters = {column: {'$regex': f'{keyword}.*$', '$options': 'i'}}
            results = self.mongodb.fetch_entries(db_name=self.file_id, collection_name=current_file_ref_id, filters=filters, skip=skip, limit=limit, get_total=True)
            is_undo_possible, is_redo_possible, profile = compute_profile_undo_redo(file_id=self.file_id, mongodb=self.mongodb, redis=self.redis)
            return {**results, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def sort(self, options: Dict[str, str]) -> Dict[Any, Any]:
        if isinstance(options, dict):
            skip = options['skip']
            limit = options['limit']
            order = 1
            current_file_ref_id = fetch_current_file_ref_id(redis=self.redis, file_id=self.file_id)
            if options['order'] == 'desc':
                order = -1
            filters = {'operation': options['operation'], 'col_name': options['col_name']}
            results = self.mongodb.fetch_entries(db_name=self.file_id, collection_name=current_file_ref_id, filters=filters, order=order, skip=skip, limit=limit, get_total=True)
            is_undo_possible, is_redo_possible, profile = compute_profile_undo_redo(file_id=self.file_id, mongodb=self.mongodb, redis=self.redis)
            return {**results, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def filter(self, options: Dict[str, str]) -> Dict[Any, Any]:
        if isinstance(options, dict):
            filters = None
            skip = options['skip']
            limit = options['limit']
            col_name = options['col']
            current_file_ref_id = fetch_current_file_ref_id(redis=self.redis, file_id=self.file_id)
            if options.get('lower_range') and options.get('higher_range'):
                filters = {col_name: {'$gte': options.get('lower_range'), '$lte': options.get('higher_range')}}
            else:
                columns = options['columns']
                filters = {col_name: {'$in': columns}}
            results = self.mongodb.fetch_entries(db_name=self.file_id, collection_name=current_file_ref_id, filters=filters, skip=skip, limit=limit, get_total=True)
            is_undo_possible, is_redo_possible, profile = compute_profile_undo_redo(file_id=self.file_id, mongodb=self.mongodb, redis=self.redis)
            return {**results, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def split(self, options: Dict[str, str]) -> Dict[Any, Any]:
        if isinstance(options, dict):
            try:
                op = Operation(file_id=self.file_id)
                action_details = create_action(action='Split', username=options['username'], args=(col, new_col1, new_col2, separator))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def merge(self, options: Dict[str, str]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                action_details = create_action(action='Merge', username=options['username'], args=(col1, col2, new_col, separator))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                print(e)
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def delete_column(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                action_details = create_action(action='Delete Column', username=options['username'], args=col)
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def rename(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                action_details = create_action(action='Rename', username=options['username'], args=(prev_col, new_col))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def calculate_value_post_waiver(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                action_details = create_action(action='Waiver Value Calculation', username=options['username'], args=(col_name, percentage))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def mask(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                action_details = create_action(action='Mask', username=options['username'], args=(column_list, no_of_unmasked_char))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def range(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                action_details = create_action(action='Range', username=options['username'], args=(col_name, lower_range, higher_range))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def ratio(self, options: Dict[str, Any]) -> Dict[str, Any]:
        if isinstance(options, dict):
            try:
                profile = data_profile(df=df)
                action_details = create_action(action='Ratio', username=options['username'], args=(col1, col2))
                response = process_operations(df=df, file_id=self.file_id, current_file_ref_id=current_file_ref_id, mongodb=self.mongodb, redis=self.redis, action_details=action_details,
                                              skip=skip, limit=limit)
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
            except Exception as e:
                print(e)
            except KeyError as e:
                return {'data': [], 'errors': f'KeyError: {e.args}. File not found.'}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def undo(self, options: Dict[str, Any]):
        linked_list = LinkedList(file_id=self.file_id, mongodb=self.mongodb)
        current_file_ref_id = fetch_current_file_ref_id(redis=self.redis, file_id=self.file_id)
        try:
            prev_node = linked_list.fetch_prev_node(file_ref_id=current_file_ref_id)
            new_file_ref_id = prev_node['_id']
            is_file_ref_id_updated = self.redis.create_entries(id=self.file_id, key='jobs', data=new_file_ref_id)
            if is_file_ref_id_updated:
                response = self.all(options=options)
                data = response['data']
                df = pd.DataFrame(data)
                profile = data_profile(df=df)
                self.mongodb.replace_one(db_name='universal', collection_name='data_profile', filters={'_id': self.file_id}, data=profile)
                prev_prev_node = linked_list.fetch_prev_node(file_ref_id=current_file_ref_id)
                is_undo_possible = bool(prev_prev_node['prev_node'])
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_undo_possible': is_undo_possible}
            else:
                return {**self.all(), 'errors': {'message': 'Could not perform the UNDO action.'}}
        except IndexError as e:
            return {'errors': {'message': 'No more UNDO actions available.'}}
        except TypeError as e:
            # Occurs when prev_node is None and '_id' key does not exist
            return {'errors': {'message': 'No more UNDO actions available.'}}

    def redo(self, options: Dict[str, Any]):
        linked_list = LinkedList(file_id=self.file_id, mongodb=self.mongodb)
        current_file_ref_id = fetch_current_file_ref_id(redis=self.redis, file_id=self.file_id)
        try:
            next_node = linked_list.fetch_next_node(file_ref_id=current_file_ref_id)
            new_file_ref_id = next_node['_id']
            is_file_ref_id_updated = self.redis.create_entries(id=self.file_id, key='jobs', data=new_file_ref_id)
            if is_file_ref_id_updated:
                response = self.all(options=options)
                data = response['data']
                df = pd.DataFrame(data)
                profile = data_profile(df=df)
                self.mongodb.replace_one(db_name='universal', collection_name='data_profile', filters={'_id': self.file_id}, data=profile)
                next_next_node = linked_list.fetch_next_node(file_ref_id=current_file_ref_id)
                is_redo_possible = bool(next_next_node['next_node'])
                return {**response, 'column_headers': profile['column_headers'], 'column_info': profile['column_info'], 'is_redo_possible': is_redo_possible}
            else:
                return {**self.all(), 'errors': {'message': 'Could not perform the REDO action.'}}
        except IndexError as e:
            return {'errors': {'message': 'No more REDO actions available.'}}
        except TypeError as e:
            # Occurs when next_node is None and '_id' key does not exist
            return {'errors': {'message': 'No more REDO actions available.'}}
