"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict

import redis

from schemas.Cache import Cache
from utils.functions import DateTimeSerializer


class Redis(Cache):
    DEFAULT_EXPIRATION_TIME = 60 * 120  # 2 hours = 120 minutes * 60 sec per minute

    def __init__(self, host: str = '127.0.0.1', port: int = 6379, db: int = 0, password: str = None):
        super().__init__()
        self.connection = redis.Redis(host=host, port=port, db=db, password=password)

    def ping(self) -> bool:
        try:
            return self.connection.ping()
        except redis.exceptions.ConnectionError as e:
            """
            Implement error logging here for Connection errors
            """
            logging.error(f'Redis - Ping Error: {e}')
            return False

    def create_entries(self, id: str, data: Any, key: str = 'session') -> Dict[str, Any]:
        try:
            return {'created': self.connection.hset(key, id, json.dumps(data, cls=DateTimeSerializer))}
        except redis.exceptions.ConnectionError as e:
            """
            Implement error logging here for Connection errors
            """
            logging.error(f'Redis - Create Entries Error: {e}')
            return {'errors': e.args}

    def create_expiring_set_entries(self, name: str, value: Any, time_in_sec: int = DEFAULT_EXPIRATION_TIME) -> bool:
        try:
            return self.connection.setex(name=name, value=json.dumps(value, cls=DateTimeSerializer), time=time_in_sec)
        except Exception as e:
            logging.error(f'Redis - Expring Entries Error: {e}')
            return False

    def fetch_entries(self, id: str = None, key: str = 'session') -> Dict[Any, Any]:
        try:
            entries = {}
            if id is None:
                # Return all sessions' info for all session IDs
                sessions = self.connection.hgetall(key)
                for dict_key, dict_value in sessions.items():
                    # Decoding ids since they are returned as byte literals
                    dict_key = dict_key.decode('utf-8')
                    entries = {**entries, **self.fetch_entries(dict_key)}
            else:
                # Return single session info for that session ID
                entries = {id: self.connection.hget(key, id)}
                if not isinstance(entries[id], dict) and entries[id] is not None:
                    entries[id] = entries[id].decode('utf-8').replace('"', '')
            return entries
        except redis.exceptions.ConnectionError as e:
            """
            Implement error logging here for Connection errors
            """
            return {'errors': e.args}
        except TypeError as e:
            return {'errors': e.args}

    def fetch_set_entries(self, name: str):
        """

        @rtype: object
        """
        entries = {}
        try:
            data = self.connection.get(name=name)
            if data is not None:
                data = json.loads(data.decode('utf-8'))
                entries = {**data}
        except Exception as e:
            logging.error(f'Redis - Fetch Set Entries Error: {e}')
        return entries

    def delete_entries(self, id: str = None, key: str = 'session') -> Dict[str, Any]:
        try:
            return {'count': self.connection.hdel(key, id)}
        except redis.exceptions.ConnectionError as e:
            """
            Implement error logging here for Connection errors
            """
            logging.error(f'Redis - Delete Entries Error: {e}')
            return {'errors': e.args}

    def expire_key(self, name: str, time_in_sec: int = DEFAULT_EXPIRATION_TIME) -> bool:
        try:
            self.connection.expire(name=name, time=time_in_sec)
            return True
        except Exception as e:
            return False

    def exists(self, name: str):
        try:
            return self.connection.exists(name)
        except Exception as e:
            logging.error(f'Redis - Exists Error: {e}')
            return False
