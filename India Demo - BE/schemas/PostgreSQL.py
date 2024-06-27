"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import psycopg2

from config.config import CIP_DBConfig
from schemas.Database import SQL


class PostgreSQL(SQL):
    def __init__(self, host: str = CIP_DBConfig.HOST, username: str = CIP_DBConfig.USERNAME, port: int = CIP_DBConfig.PORT, db: str = CIP_DBConfig.DB_NAME, password: str = CIP_DBConfig.PASSWORD):
        super().__init__()
        self.connection = psycopg2.connect(host=host, port=port, database=db, user=username, password=password)
        self.cursor = self.connection.cursor()

    def ping(self) -> bool:
        try:
            self.cursor.execute("SELECT version()")
            ping = self.cursor.fetchone()
            if len(ping) > 0:
                return True
            return False
        except Exception as e:
            """
            Implement error logging here for Connection errors
            """
            print(e.args)
            return False

    def create_entries(self, table_name: str, options: Dict[str, Any], columns: List[str]) -> bool:
        values = []
        value_string = '('
        query = f'INSERT INTO {table_name} ('
        columns = [*columns, 'batch_no', 'customer_id']
        try:
            for column in columns:
                query += f'{column}, '
                value_string += '%s, '
            value_string = value_string[:-2] + ');'
            query = query[:-2] + ') VALUES ' + value_string
            for row in options['data']:
                temp = list()
                if 'mappings' in options:
                    for k, v in options['mappings'].items():
                        temp.append(row[k])
                else:
                    for column in columns:
                        temp.append(row[column])
                temp = tuple(temp)
                values.append(temp)
            self.cursor.executemany(query, values)
            self.connection.commit()
            self.cursor.close()
            self.connection.close()
            return True
        except Exception as e:
            logging.error(f'Error creating entries to PostgreSQL: {e}')
            return False

    def fetch_entries(self, table_name: str, options: Dict[str, Any] = None, columns: List[str] = None, operator: str = 'AND') -> Dict[Any, Any]:
        """
        options = {'WHERE': [{col1: val1}, {col2: val2}, ... ]}
        """
        query = 'SELECT '
        query_params = []
        try:
            if columns is None:
                query += '* '
            else:
                column_param = ''
                for column in columns:
                    column_param += f'{column}, '
                query += column_param
                query = query[:-2]
            query += f' FROM {table_name}'
            for k, v in options.items():
                if k == 'WHERE' and isinstance(v, list):
                    query += ' WHERE '
                    for condition in v:
                        for key, value in condition.items():
                            if isinstance(value, list):
                                value_str = ''
                                add_null = False
                                for elem in value:
                                    if elem is None:
                                        add_null = True
                                    value_str += '%s, '
                                    query_params.append(elem)
                                value_str = value_str[:-2]
                                if add_null:
                                    query += f'({key} IN ({value_str}) OR {key} is NULL) '
                                else:
                                    query += f'{key} IN ({value_str}) '
                            else:
                                if isinstance(value, str):
                                    query += f"{key} = '{value}' "
                                elif isinstance(value, tuple):
                                    # Tuple is for cases where conditions are needed - as in >, <, >=, <=
                                    value_condition, value_query = value
                                    query += f'{key} {value_condition} {value_query} '
                                else:
                                    query += f'{key} = {value} '
                            query += f'{operator} '
                    query = query[:-5]
                elif k in ['GROUP BY', 'ORDER BY']:
                    if isinstance(v, str):
                        query += f' {k} {v}'
                    elif isinstance(v, list):
                        group_by_params = ', '.join(map(str, v))
                        query += f' {k} {group_by_params}'
            query += ';'
            self.cursor.execute(query)
            data = self.cursor.fetchall()
            total = self.cursor.rowcount
            self.cursor.close()
            self.connection.close()
            return {'data': data, 'total': total}
        except TypeError as e:
            return {'errors': e.args}

    def delete_entries(self):
        pass
