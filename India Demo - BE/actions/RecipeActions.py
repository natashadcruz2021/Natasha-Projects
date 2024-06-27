"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from dataclasses import asdict
from datetime import datetime
from typing import Any, Dict, List

from constants import VALID_STATUS_COL
from models.Recipe import Recipe
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.classes.LinkedList import LinkedList
from utils.functions import difference_between_lists
from utils.functions.jobs import fetch_current_job_ref_id


class RecipeActions:
    def __init__(self, job_id: str):
        self.mongodb = MongoDB()
        self.collection_name = 'recipes'
        self.job_id = job_id

    def fetch(self, order: str = 'asc') -> List[Dict[str, Any]]:
        linked_list = LinkedList(job_id=self.job_id, mongodb=self.mongodb)
        recipe = linked_list.fetch_all_nodes(order=order)
        return recipe

    def fetch_by_reference_id(self, job_reference_id: str) -> Dict[str, Any] | None:
        filters = {'job_id': self.job_id, '_id': job_reference_id}
        existing_recipe = self.mongodb.fetch_entries(collection_name=self.collection_name, filters=filters, limit=1)
        if len(existing_recipe['data']) > 0:
            return existing_recipe['data'][0]
        return None

    def add(self, action_details: Dict[str, Any], new_job_ref_id: str, current_job_ref_id: str = None,
            column_mapping: Dict[str, str] = None, rev_column_mapping: Dict[str, str] = None):
        filters = {'job_id': self.job_id}
        update_filter = None
        update_params = None
        existing_recipe = self.mongodb.fetch_entries(collection_name=self.collection_name, filters=filters, limit=1)
        if existing_recipe['total'] == 0:
            current_job_ref_id = None
        else:
            filters['next_node'] = None
            existing_recipe = self.mongodb.fetch_entries(collection_name=self.collection_name, filters=filters, limit=1)
            existing_recipe_id = existing_recipe['data'][0]['_id']
            update_filter = {'_id': existing_recipe_id}
            update_params = {'next_node': new_job_ref_id}
        extra_cols_count = 0
        if '_id' in rev_column_mapping:
            extra_cols_count += 1
        if VALID_STATUS_COL in rev_column_mapping:
            extra_cols_count += 1
        recipe = Recipe(
            _id=new_job_ref_id,
            action=action_details['action'],
            description=action_details['description'],
            created_date=datetime.today().replace(microsecond=0),
            job_id=self.job_id,
            prev_node=current_job_ref_id,
            column_mapping=column_mapping,
            rev_column_mapping=rev_column_mapping,
            col_count=len(rev_column_mapping) - extra_cols_count
        )
        recipe_data = [asdict(recipe)]
        is_new_recipe_created = self.mongodb.create_entries(collection_name='recipes', data=recipe_data)
        if is_new_recipe_created:
            self.mongodb.update_one(collection_name=self.collection_name, filter=update_filter,
                                    update_params=update_params)
        return is_new_recipe_created

    def edit(self):
        pass

    def duplicate_and_edit(self):
        pass

    def deactivate(self, job_id: str):
        return self.mongodb.update_many(collection_name=self.collection_name, filter={'job_id': job_id},
                                        update_params={'status': False})

    def finalize(self, order: str = 'asc'):
        """
        Find current file_ref_ref_id from cache -> Fetch the linked_list for the current job_id ->
        Create a new list till the current file_ref_id -> Delete the nodes that are no longer part of the recipe ->
        Update the last node's next_node to NULL
        """
        linked_list = self.fetch(order='asc')
        redis = Redis(db=1)
        file_ref_id = fetch_current_job_ref_id(redis=redis, job_id=self.job_id)
        recipe = []
        for node in linked_list:
            recipe.append(node)
            if node['_id'] == file_ref_id:
                break
        final_recipe = difference_between_lists(array_1=linked_list, array_2=recipe)
        ids_to_remove = []
        for node in final_recipe:
            ids_to_remove.append(node['_id'])
        self.mongodb.delete_many(collection_name=self.collection_name, filter={'_id': {'$in': ids_to_remove}})
        self.mongodb.update_one(collection_name=self.collection_name, filter={'_id': file_ref_id},
                                update_params={'next_node': None})
        if order == 'desc':
            recipe = recipe[::-1]
        return recipe
