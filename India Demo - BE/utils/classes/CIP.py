"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
from datetime import datetime
from typing import Any, Dict, List, Tuple

# from cip_process.spFileLoading_v5 import create_new_table, run_cleaning_process
# from engage_load import engage_v4
from schemas.PostgreSQL import PostgreSQL


class CIP:
    def __init__(self, config: object = None):
        if config is not None:
            self.pg = PostgreSQL(host=config.HOST, username=config.USERNAME, port=config.PORT, db=config.DB_NAME, password=config.PASSWORD)
            self.customer_id = '99'
            self.customer_name = 'SMARTBank'

    @staticmethod
    def create_table(file_id: str, batch_no: int) -> Tuple:
        today_date = datetime.today().strftime("%d_%m_%Y_%H_%M_%S")
        new_file_id = file_id.replace('-', '')
        table_name = f'cip_organize.st_{batch_no}_{new_file_id}_{today_date}'
        logging.info(f'Table Name: {table_name}')
        is_table_created = True
        # is_table_created = create_new_table(table_name=table_name)
        return is_table_created, table_name

    def cleaning_and_engage_load(self, table_name: str, batch_no: int, file_id: str, columns: List[str], data: List[Dict[str, Any]]) -> bool:
        is_data_moved = self.pg.create_entries(table_name=table_name, options={'data': data}, columns=columns)
        logging.critical(f'IS DATA MOVED: ')
        if is_data_moved:
            try:
                # run_cleaning_process(table_name=table_name, batch_no=batch_no)
                # engage_v4.load(client_id=self.customer_id, client_name=self.customer_name, batch_no=batch_no, table_name=table_name, file_id=file_id)
                return True
            except Exception as e:
                logging.error(f'Cleaning process failed: {e}')
                return False
    