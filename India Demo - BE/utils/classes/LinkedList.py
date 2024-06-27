"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from typing import Any, Dict, List

from schemas.MongoDB import MongoDB


class LinkedList:
    def __init__(self, job_id: str, mongodb: MongoDB):
        self.mongodb = mongodb
        self.collection_name = 'recipes'
        self.job_id = job_id

    def fetch_current_node(self, job_ref_id: str) -> Dict[str, Any] | None:
        try:
            entry = self.mongodb.fetch_entries(
                collection_name=self.collection_name,
                filters={'job_id': self.job_id, '_id': job_ref_id},
                limit=1
            )
            node = entry['data'][0]
            return node
        except IndexError as e:
            return None

    def fetch_head_node(self) -> Dict[str, Any] | None:
        entry = self.mongodb.fetch_entries(
            collection_name=self.collection_name,
            filters={'job_id': self.job_id, 'prev_node': None},
            limit=1
        )
        if isinstance(entry['data'], list) and len(entry['data']) == 0:
            return entry
        node = entry['data'][0]
        return node

    def fetch_tail_node(self) -> Dict[str, Any]:
        entry = self.mongodb.fetch_entries(
            collection_name=self.collection_name,
            filters={'job_id': self.job_id, 'next_node': None},
            limit=1
        )
        node = entry['data'][0]
        return node

    def fetch_prev_node(self, current_node: Dict[str, Any] = None, job_ref_id: str = None) -> Dict[str, Any] | None:
        try:
            if current_node is None:
                current_node = self.fetch_current_node(job_ref_id=job_ref_id)
            prev_entry = self.mongodb.fetch_entries(
                collection_name=self.collection_name,
                filters={'job_id': self.job_id, '_id': current_node['prev_node']},
                limit=1
            )
            prev_node = prev_entry['data'][0]
            return prev_node
        except IndexError as e:
            return None

    def fetch_next_node(self, current_node: Dict[str, Any] = None, job_ref_id: str = None) -> Dict[str, Any] | None:
        try:
            if current_node is None:
                current_node = self.fetch_current_node(job_ref_id=job_ref_id)
            next_entry = self.mongodb.fetch_entries(
                collection_name=self.collection_name,
                filters={'job_id': self.job_id, '_id': current_node['next_node']},
                limit=1
            )
            next_node = next_entry['data'][0]
            return next_node
        except IndexError as e:
            return None

    def fetch_all_nodes(self, order: str = 'asc') -> List[Dict[str, Any]]:
        recipe = []
        if order == 'desc':
            tail = self.fetch_tail_node()
            if tail is not None:
                recipe.append(tail)
                while 'prev_node' in tail and tail['prev_node'] is not None:
                    current_node = self.fetch_prev_node(current_node=tail)
                    recipe.append(current_node)
                    tail = current_node
        else:
            head = self.fetch_head_node()
            if head is not None:
                recipe.append(head)
                while 'next_node' in head and head['next_node'] is not None:
                    current_node = self.fetch_next_node(current_node=head)
                    recipe.append(current_node)
                    head = current_node
        try:
            if recipe[0]['total'] == 0:
                return [recipe[0]]
        except KeyError:
            pass
        return recipe
