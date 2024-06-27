"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List

import mysql.connector

from config.config import MS_DBConfig
from schemas.Database import SQL


class MySQL(SQL):
    def __init__(self, host: str = MS_DBConfig.HOST, username: str = MS_DBConfig.USERNAME, port: int = MS_DBConfig.PORT,
                 db: str = MS_DBConfig.DB_NAME, password: str = MS_DBConfig.PASSWORD):
        super().__init__()
        self.connection = mysql.connector.connect(host=host, port=port, database=db, user=username, password=password)
        self.cursor = self.connection.cursor()

    def ping(self) -> bool:
        try:
            ping = self.connection.ping(reconnect=False, attempts=1, delay=0)
            logging.info(f'Ping: {ping}')
            return True
        except Exception as e:
            """
            Implement error logging here for Connection errors
            """
            print(e.args)
            return False

    def run_query(self, query: str, commit_result=True) -> bool:
        try:
            self.cursor.execute(query)
            if commit_result:
                self.connection.commit()
                self.cursor.close()
                self.connection.close()
            return True
        except Exception as e:
            logging.error(f'Error creating entries to MySQL: {e}')
            return False

    def create_entries(self, table_name: str, options: Dict[str, Any], columns: List[str], commit_result=True) -> bool:
        values = []
        value_string = '('
        query = f'INSERT INTO {table_name} ('
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
            if commit_result:
                self.connection.commit()
                self.cursor.close()
                self.connection.close()
            return True
        except Exception as e:
            logging.error(f'Error creating entries to MySQL: {e}')
            return False

    def update_entries(self, table_name: str, options: Dict[str, Any] = None, operator: str = 'AND',
                       commit_result=True) -> bool:
        values = []
        query = f'UPDATE {table_name} SET '
        try:
            for k, v in options['data'].items():
                query = query + f"{k} = %s ,"
                values.append(v)
            query = query[:-1]
            if options.get('WHERE') and isinstance(options['WHERE'], list):
                query += ' WHERE '
                for condition in options['WHERE']:
                    for key, value in condition.items():
                        if isinstance(value, list):
                            value_str = ''
                            for elem in value:
                                value_str += f'"{str(elem)}", '
                            value_str = value_str[:-2]
                            query += f'{key} IN ({value_str}) '
                        else:
                            if isinstance(value, str):
                                query += f'{key} = "{value}" '
                            else:
                                query += f'{key} = {value} '
                        query += f'{operator} '
                query = query[:-5]
            self.cursor.execute(query, values)
            if commit_result:
                self.connection.commit()
                self.cursor.close()
                self.connection.close()
            return True
        except Exception as e:
            logging.error(f'Error creating entries to MySQL: {e}')
            return False

    def fetch_entries(self, table_name: str, options: Dict[str, Any] = None, columns: List[str] = None,
                      operator: str = 'AND') -> Dict[Any, Any]:
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
                                    query += f'{key} = "{value}" '
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
            self.cursor.execute(query, query_params)
            data = self.cursor.fetchall()
            total = self.cursor.rowcount
            return {'data': data, 'total': total}
        except TypeError as e:
            return {'errors': e.args}

    def delete_entries(self):
        pass

    def close_cursor_and_connection(self) -> None:
        self.cursor.close()
        self.connection.close()
