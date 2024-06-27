"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import json
import logging

from requests import request
from typing import Any, Dict

from config.config import CustomerDetailsConfig
from schemas.MySQL import MySQL

table_names = {
    'SFTP': 'sftpConfig',
    'Email': 'email'
}


class CustomerPortfolio:
    def __init__(self, type: str = None):
        self.type = type

    def get_table_name(self):
        return table_names.get(self.type)

    def get_sftp_credentials(self, product: str):
        table_name = self.get_table_name()
        options = {'WHERE': [{'productName': product}]}
        sql = MySQL()
        data = sql.fetch_entries(table_name=table_name, options=options)
        return data

    @staticmethod
    def fetch_batch_no(query_params: Dict[str, Any]) -> int:
        batch_no = None
        try:
            url = f'{CustomerDetailsConfig.DEV_BASE_URL}/{CustomerDetailsConfig.GENERATE_BATCH_URL}'
            payload = json.dumps(query_params)
            headers = {'Content-Type': 'application/json'}
            response = request('POST', url, headers=headers, data=payload)
            response = json.loads(response.text)
            if 'batch_no' in response.keys():
                batch_no = response['batch_no']
        except Exception as e:
            logging.error(f'Batch No API Error: {e}')
        return batch_no
