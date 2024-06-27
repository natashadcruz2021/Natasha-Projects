"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import json
import logging
from datetime import datetime
from typing import Any, Dict, List

from bson import json_util
from pymongo import MongoClient, errors, UpdateOne
from pymongo.errors import ServerSelectionTimeoutError

from constants import SUMMARY_PROPERTY_MAX_MIN
from schemas.Database import NoSQL


class MongoDB(NoSQL):
    def __init__(self, host: str = '127.0.0.1', port: int = 27017, username: str = None, password: str = None):
        super().__init__()
        self.connection = MongoClient(host=host, port=port, username=username, password=password, connectTimeoutMS=5000)

    def ping(self) -> Dict[str, Any]:
        try:
            return self.connection.admin.command('ping')
        except errors.ConnectionFailure as e:
            """
            Implement error logging here for Connection errors
            """
            logging.error(f'MongoDB Error -> Ping Method: {e}')
            return {'ok': 0.0}

    def get_server_info(self) -> ServerSelectionTimeoutError | Any:
        try:
            return self.connection.server_info()
        except errors.ServerSelectionTimeoutError as e:
            """
            Implement error logging here for Database errors
            """
            logging.error(f'MongoDB Error -> Server Info Method: {e}')
            return e

    def fetch_databases(self):
        return self.connection.list_database_names()

    def delete_collection(self, collection_name: str, db_name: str = 'universal') -> bool:
        pass

    def delete_database(self, db_name: str):
        return self.connection.drop_database(db_name)

    def fetch_collections(self, db_name: str = 'universal'):
        return self.connection[db_name].list_collection_names()

    def create_index(self, collection_name: str, db_name: str = 'universal'):
        return self.connection[db_name][collection_name].create_index([('$**', 'text')])

    def create_entries(self, collection_name: str, data: List[Dict[str, Any]], db_name: str = 'universal') -> bool:
        cursor = None
        acknowledged = False
        try:
            cursor = self.connection[db_name][collection_name].insert_many(data)
            return True
        except errors.BulkWriteError as e:
            """
            Implement error logging here for Database errors
            """
            logging.error(f'MongoDB Error -> Create Entries Method: {e}')
        return acknowledged

    def fetch_entries(self, collection_name: str, filters: Dict[str, Any] = None, db_name: str = 'universal',
                      skip: int = 0, limit: int = 0, order: int = 1, projection: List[str] = None,
                      get_total: bool = False) -> Dict[Any, Any]:
        if filters is None:
            filters = {}
        try:
            if filters.get('operation') == 'sort':
                col_name = filters['col_name']
                filters.pop('operation')
                filters.pop('col_name')
                cursor = self.connection[db_name][collection_name].find(filters, projection=projection).sort(col_name,
                                                                                                             order).skip(
                    skip).limit(limit)
            else:
                cursor = self.connection[db_name][collection_name].find(filters, projection=projection).skip(
                    skip).limit(limit)
            cursor = list(cursor)
            for row in cursor:
                for key, value in row.items():
                    if isinstance(value, datetime):
                        row[key] = value.strftime('%d/%m/%Y, %H:%M:%S%p')
            # Converting BSON to JSON
            data = json.loads(json_util.dumps(cursor))
            if get_total:
                total = self.connection[db_name][collection_name].count(filter=filters)
            else:
                total = len(data)
            return {'data': data, 'total': total}
        except TypeError as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Fetch Entries Method: {e}')
            return {'errors': e}

    def fetch_distinct_entries(self, collection_name: str, col_name: str, db_name: str = 'universal'):
        cursor = None
        try:
            cursor = []
            values = self.connection[db_name][collection_name].distinct(key=f'{col_name}.value')
            for value in values:
                if isinstance(value, datetime):
                    cursor.append(value.strftime('%d/%m/%Y, %H:%M:%S%p'))
                else:
                    cursor.append(value)
        except Exception as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Fetch Distinct Entries Method: {e}')
        return cursor

    def replace_one(self, collection_name: str, filters: Dict[str, Any], data: Dict[str, Any],
                    db_name: str = 'universal'):
        cursor = None
        try:
            cursor = self.connection[db_name][collection_name].replace_one(filters, data)
        except TypeError as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Replace One Method: {e}')
        return cursor

    def update_one(self, collection_name: str, filter: Dict[str, Any], update_params: Dict[str, Any],
                   db_name: str = 'universal'):
        acknowledged = False
        try:
            self.connection[db_name][collection_name].update_one(filter, {'$set': {**update_params}})
            acknowledged = True
        except TypeError as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Update One Method: {e}')
        return acknowledged

    def update_many(self, collection_name: str, filter: Dict[str, Any], update_params: Dict[str, Any],
                    db_name: str = 'universal') -> Dict[str, Any]:
        cursor = None
        try:
            cursor = self.connection[db_name][collection_name].update_many(filter, {'$set': {**update_params}})
        except TypeError as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Update Many Method: {e}')
        return cursor

    def delete_many(self, collection_name: str, filter: Dict[str, Any], db_name: str = 'universal'):
        cursor = None
        try:
            cursor = self.connection[db_name][collection_name].delete_many(filter)
        except TypeError as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Delete Many Method: {e}')
        return cursor

    # docs is a list of docs that needs to be updated
    # each doc should have _id field and new fields to be added
    def upsert_many(self, collection_name: str, docs: List[Dict[str, Any]], db_name: str = 'universal') -> Dict[
        str, Any]:
        cursor = None
        try:
            write_reqs = []
            for doc in docs:
                if "_id" not in doc.keys():
                    continue
                for key, value in doc.items():
                    if key == "_id":
                        continue
                    req = UpdateOne({"_id": doc["_id"]}, {'$set': {key: value}})
                    write_reqs.append(req)
            cursor = self.connection[db_name][collection_name].bulk_write(write_reqs)
            print(cursor.bulk_api_result)
        except TypeError as e:
            logging.error(f'MongoDB Error -> Upsert Many Method: {e}')
        return cursor

    def aggregate(self, collection_name: str, filter: Dict[str, Any] = None, type: str = 'count', col_name: str = None, db_name: str = 'universal'):
        aggregation = None
        try:
            if type == SUMMARY_PROPERTY_MAX_MIN:
                """
                  - Included an '_id' in the query below as the absence of it gives the following error:
                    "a group specification must include an _id, full error:
                    {'ok': 0.0, 'errmsg': 'a group specification must include an _id', 'code': 15955, 'codeName': 'Location15955'}
                    "
                """
                filter = [{'$group': {'_id': None, 'max': {'$max': f'${col_name}.value'}, 'min': {'$min': f'${col_name}.value'}}}]
                aggregation = self.connection[db_name][collection_name].aggregate(filter)
                aggregation = list(aggregation)
            else:
                aggregation = self.connection[db_name][collection_name].count_documents(filter=filter)
        except Exception as e:
            """
            Implement error logging here for Type errors
            """
            logging.error(f'MongoDB Error -> Aggregate Method: {e}')
        return aggregation
