"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

from actions.RecipeActions import RecipeActions
from constants import VALID_STATUS_COL, REV_COLUMN_MAPPING_COL, COL_MAPPING_COL
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.Job import Job
from utils.classes.LinkedList import LinkedList
from utils.functions import get_datatype_appropriate_values
from utils.functions.analytics import compute_column_headers
from utils.functions.jobs import fetch_current_job_ref_id
from utils.functions.operations import compute_undo_redo
from utils.functions.operations import map_columns


class Filters:
    def __init__(self, job_id: str):
        self.redis = Redis(db=1)
        self.mongodb = MongoDB()
        self.job_id = job_id
        self.job = Job(mongodb=self.mongodb)
        self.recipe_actions = RecipeActions(self.job_id)

    def all(self, skip: int = 0, limit: int = 0, projection: List[str] = None) -> Dict[str, Any]:
        current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
        recipe = self.recipe_actions.fetch_by_reference_id(current_job_ref_id)
        final_projection = None
        column_mapping = recipe[COL_MAPPING_COL]
        if projection is not None:
            column_mapping = {'_id': '_id'}
            final_projection = []
            for p in projection:
                final_projection.append(recipe[REV_COLUMN_MAPPING_COL][p])
                column_mapping[recipe[REV_COLUMN_MAPPING_COL][p]] = p
        results = self.mongodb.fetch_entries(collection_name=self.job_id, filters={}, skip=skip, limit=limit, projection=final_projection, get_total=True)
        results['data'] = map_columns(results['data'], column_mapping)
        is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, redis=self.redis, mongodb=self.mongodb)
        results['column_headers'] = compute_column_headers(data=results['data'])
        results['header_mapping_datatype'] = self.job.fetch_job_detail(id=self.job_id, param='header_mapping_datatype')
        if len(results['column_headers']) == 0:
            results['column_headers'] = self.job.fetch_job_detail(id=self.job_id, param='headers')
        return {**results, 'job_ref_id': current_job_ref_id, 'is_undo_possible': is_undo_possible,
                'is_redo_possible': is_redo_possible}

    def search(self, skip: int, limit: int, query: str, col: str = None) -> Dict[Any, Any]:
        current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
        recipe = self.recipe_actions.fetch_by_reference_id(current_job_ref_id)
        if col is None:
            # Creating an index to allow for full-text search
            self.mongodb.create_index(collection_name=self.job_id)
            filters = {'$text': {'$search': query}}
        else:
            col = recipe[REV_COLUMN_MAPPING_COL][col]
            filters = {f'{col}.value': {'$regex': f'{query}.*$', '$options': 'i'}}
        results = self.mongodb.fetch_entries(collection_name=self.job_id, filters=filters, skip=skip, limit=limit, get_total=True)
        results['data'] = map_columns(results['data'], recipe[COL_MAPPING_COL])
        is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                               redis=self.redis)
        results['column_headers'] = compute_column_headers(data=results['data'], job_id=self.job_id)
        results['header_mapping_datatype'] = self.job.fetch_job_detail(id=self.job_id, param='header_mapping_datatype')
        return {**results, 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}

    def sort(self, skip: int, limit: int, col: str, order: int = 1) -> Dict[Any, Any]:
        current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
        recipe = self.recipe_actions.fetch_by_reference_id(current_job_ref_id)
        col = recipe[REV_COLUMN_MAPPING_COL][col]
        filters = {'operation': 'sort', 'col_name': f'{col}.value'}
        results = self.mongodb.fetch_entries(collection_name=self.job_id, filters=filters, order=order, skip=skip,
                                             limit=limit, get_total=True)
        results['data'] = map_columns(results['data'], recipe[COL_MAPPING_COL])
        is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb,
                                                               redis=self.redis)
        results['column_headers'] = compute_column_headers(data=results['data'])
        results['header_mapping_datatype'] = self.job.fetch_job_detail(id=self.job_id, param='header_mapping_datatype')
        return {**results, 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}

    def filter(self, skip: int, limit: int, filter_args: Dict[str, str]) -> Dict[Any, Any]:
        if isinstance(filter_args, dict):
            current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
            filters = None
            col = filter_args['col']
            recipe = self.recipe_actions.fetch_by_reference_id(current_job_ref_id)
            col = recipe[REV_COLUMN_MAPPING_COL][col]
            if filter_args.get('lower_range') and filter_args.get('higher_range'):
                lower_range = int(filter_args.get('lower_range'))
                higher_range = int(filter_args.get('higher_range'))
                filters = {f'{col}.value': {'$gte': lower_range, '$lte': higher_range}}
            elif filter_args.get('values'):
                values = filter_args.get('values')
                datatype = filter_args.get('datatype')
                values = values.split(',')
                """
                Added this for cases where integer was received as string in query params.
                While it solves that issue, it causes issue with "Mobile Number" datatype which
                is numeric in nature, treated as text.
                """
                values = get_datatype_appropriate_values(array=values, datatype=datatype)
                filters = {f'{col}.value': {'$in': values}}
            results = self.mongodb.fetch_entries(collection_name=self.job_id, filters=filters, skip=skip, limit=limit, get_total=True)
            is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb, redis=self.redis)
            return {**results, 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
        else:
            return {'data': [], 'errors': 'Incorrect input received.'}

    def undo(self):
        linked_list = LinkedList(job_id=self.job_id, mongodb=self.mongodb)
        current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
        try:
            prev_node = linked_list.fetch_prev_node(job_ref_id=current_job_ref_id)
            new_job_ref_id = prev_node['_id']
            is_job_ref_id_updated = self.redis.create_entries(id=self.job_id, key='jobs', data=new_job_ref_id)
            if is_job_ref_id_updated:
                prev_prev_node = linked_list.fetch_prev_node(job_ref_id=current_job_ref_id)
                is_undo_possible = bool(prev_prev_node['prev_node'])
                return {'is_undo_possible': is_undo_possible}
            else:
                return {**self.all(), 'errors': {'message': 'Could not perform the UNDO action.'}}
        except IndexError as e:
            return {'errors': {'message': 'No more UNDO actions available.'}}
        except TypeError as e:
            # Occurs when prev_node is None and '_id' key does not exist
            return {'errors': {'message': 'No more UNDO actions available.'}}

    def redo(self):
        linked_list = LinkedList(job_id=self.job_id, mongodb=self.mongodb)
        current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
        try:
            next_node = linked_list.fetch_next_node(job_ref_id=current_job_ref_id)
            new_job_ref_id = next_node['_id']
            is_job_ref_id_updated = self.redis.create_entries(id=self.job_id, key='jobs', data=new_job_ref_id)
            if is_job_ref_id_updated:
                next_next_node = linked_list.fetch_next_node(job_ref_id=current_job_ref_id)
                is_redo_possible = bool(next_next_node['next_node'])
                return {'is_redo_possible': is_redo_possible}
            else:
                return {**self.all(), 'errors': {'message': 'Could not perform the REDO action.'}}
        except IndexError as e:
            return {'errors': {'message': 'No more REDO actions available.'}}
        except TypeError as e:
            # Occurs when next_node is None and '_id' key does not exist
            return {'errors': {'message': 'No more REDO actions available.'}}

    def invalid(self, skip: int = 0, limit: int = 0) -> Dict[str, Any]:
        current_job_ref_id = fetch_current_job_ref_id(redis=self.redis, job_id=self.job_id)
        recipe = self.recipe_actions.fetch_by_reference_id(current_job_ref_id)
        is_valid_col_name = recipe[REV_COLUMN_MAPPING_COL][VALID_STATUS_COL]
        results = self.mongodb.fetch_entries(collection_name=self.job_id, filters={is_valid_col_name: False}, skip=skip, limit=limit, get_total=True)
        results['data'] = map_columns(results['data'], recipe[COL_MAPPING_COL])
        column_headers = compute_column_headers(data=results['data'])
        if isinstance(column_headers, list) and len(column_headers) == 0:
            column_headers = self.job.fetch_job_detail(id=self.job_id, param='headers')
        return {**results, 'column_headers': column_headers}
