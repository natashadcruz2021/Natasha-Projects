"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import json
from typing import Any, Dict, Tuple, List
from uuid import uuid4

from schemas.Redis import Redis


class Session:
    def __init__(self):
        self.redis = Redis()
        self.redis_job_ref = Redis(db=1)
        self.errors = []

    def create_session(self, user_info: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Any]:
        unique_id = str(uuid4())
        session_info = self.redis.create_entries(unique_id, user_info)
        if 'errors' in session_info:
            self.errors = session_info['errors']
            session_info = {}
        return unique_id, session_info, self.errors

    def fetch_session(self, id: str = None) -> Tuple[Dict[str, Any], List[Any]]:
        session_info = self.redis.fetch_entries(id)
        """
        Converting the 'session_info' list of dictionaries with 'key': 'value' pairs 
        in the byte literal format, to string literal using list comprehension.
        """
        if 'errors' in session_info:
            self.errors = session_info['errors']
            session_info = {}
        else:
            session_info = {key: json.loads(value.decode('utf-8')) for key, value in session_info.items()}
        return session_info, self.errors

    def delete_session(self, id: str) -> Tuple[Dict[str, Any], List[Any]]:
        session_info = self.redis.delete_entries(id)
        if 'errors' in session_info:
            self.errors = session_info['errors']
            session_info = {}
        return session_info, self.errors
