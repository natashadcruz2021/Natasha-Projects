"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
from elasticsearch import Elasticsearch, helpers, exceptions
import pandas as pd
from typing import Any, Dict

from config.config import ES_DBConfig
# from schemas.Database import NoSQL


class ES:
    def __init__(self, host: str = ES_DBConfig.HOST, username: str = ES_DBConfig.USERNAME, password=ES_DBConfig.PASSWORD):
        super().__init__()
        self.connection = Elasticsearch(hosts=[host], http_auth=(username, password), timeout=600)

    def ping(self):
        print(self.connection.info)
        # "not" has been added as for successful connection ES returns False
        return not self.connection.ping()

    def create_index(self, name: str, schema: Dict[str, Any]) -> bool:
        return self.connection.indices.create(index=name, mappings=schema)

    def fetch_entries(self, query: Dict[str, Any], index_name: str = ES_DBConfig.TRACE_INDEX, size: int = 5000):
        data = helpers.scan(self.connection, scroll='5m', size=size, index=index_name, query=query, preserve_order=False, request_timeout=3000)
        # df = pd.DataFrame([i['_source'] for i in data])
        # data = df.to_dict(orient='record')
        return data
