"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

import logging
import math
from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from flask import current_app

from config.config import ENGAGE_DBConfig
from constants import ENGAGE_DISPOSITION_COL, ENGAGE_LOAN_ACC_COL, ENGAGE_TABLE_NAME, PAYMENT_FILE_AMOUNT_COLUMNS, PAYMENT_FILE_MAPPING, PAYMENT_FILE_PAID_STATUS, PAYMENT_FOLDER, PAYMENT_LOG_STATUS_COL, PAYMENT_LOG_TABLE_NAME
from schemas.MySQL import MySQL
from utils.classes.Job import Job
from utils.functions import get_dict_keys_from_list
from utils.functions import read_file
from utils.functions.analytics import map_column_headers
from utils.functions.helper import create_chunks

CHUNK_SIZE = 1000


class PaymentFile:
    def __init__(self, job_id: str):
        self.job_id = job_id
        self.FOLDER_PATH = current_app.config[PAYMENT_FOLDER]

    def validate(self, user_mapping: Dict[str, str], file_name: str) -> Tuple[bool, str]:
        df = read_file(file_name, folder_type=PAYMENT_FOLDER)
        df = map_column_headers(map_column_headers(df, user_mapping), PAYMENT_FILE_MAPPING)
        has_disposition = False
        has_account_number = False
        amount_column_count = 0

        if ENGAGE_DISPOSITION_COL in df.columns:
            has_disposition = True
        if ENGAGE_LOAN_ACC_COL in df.columns:
            has_account_number = True
        for ac in PAYMENT_FILE_AMOUNT_COLUMNS:
            if ac in df.columns:
                amount_column_count += 1

        if has_disposition is True and has_account_number is True and amount_column_count == 0:
            for idx, row in df.iterrows():
                if isinstance(row[ENGAGE_DISPOSITION_COL], str) is False:
                    return False, "Invalid payment status in file"
            return True, ""

        if has_disposition is False and has_account_number is True and amount_column_count == 1:
            return True, ""

        return False, "Invalid column mapping"

    def get_user_mapping(self, file_name: str) -> Tuple[Dict[str, str], List[str]]:
        df = pd.read_pickle(f'{self.FOLDER_PATH}/{file_name}')
        headers = list(df.columns)
        job = Job()
        user_mapping = {}
        try:
            user_mapping = job.fetch_job_detail(id=self.job_id, param='user_mapping')
        except KeyError as e:
            logging.error(f'Payment File -> Get Mapping -> Job ID: {self.job_id} -> Error: {e}')
        new_user_mapping = get_dict_keys_from_list(mapping=user_mapping, headers=headers)
        return new_user_mapping, headers

    def process_partial_payment(self, sales_order_number: int, df) -> bool:
        if df.shape[0] == 0:
            return True

        mysql_conn = MySQL(host=ENGAGE_DBConfig.HOST, port=ENGAGE_DBConfig.PORT, username=ENGAGE_DBConfig.USERNAME,
                           password=ENGAGE_DBConfig.PASSWORD, db=ENGAGE_DBConfig.DB_NAME)
        logging.debug(f'Payment: MySQL Connection: {mysql_conn}')

        amount_col = ''
        for ac in PAYMENT_FILE_AMOUNT_COLUMNS:
            if ac in df.columns:
                amount_col = ac
                break

        df = df[[ENGAGE_LOAN_ACC_COL, amount_col]]

        query = self.engage_bulk_update_query(df, amount_col)

        done = mysql_conn.run_query(query, commit_result=False)
        if done is False:
            return False

        payment_log_data = []
        for idx, row in df.iterrows():
            payment_log = {
                "sales_order_no": sales_order_number,
                ENGAGE_LOAN_ACC_COL: row[ENGAGE_LOAN_ACC_COL],
                "payment_amount": row[amount_col]
            }
            payment_log_data.append(payment_log)

        insert_options = {
            "data": payment_log_data
        }

        return mysql_conn.create_entries(PAYMENT_LOG_TABLE_NAME, insert_options,
                                         ["sales_order_no", ENGAGE_LOAN_ACC_COL, "payment_amount"])

    def engage_bulk_update_query(self, df, amount_col):
        query_string = f'''UPDATE {ENGAGE_TABLE_NAME} inner join ('''
        select_query = ""
        for idx, r in df.iterrows():
            select_query += "select '%s' as loan_account_no,  '%s' as new_paid_amount UNION ALL " % (
            r[ENGAGE_LOAN_ACC_COL], r[amount_col])

        query_string += select_query[:-10]
        query_string += f" ) as tmp on tmp.loan_account_no = {ENGAGE_TABLE_NAME}.{ENGAGE_LOAN_ACC_COL}"
        query_string += f''' SET 
                    {ENGAGE_DISPOSITION_COL} = case when new_paid_amount = %s then 'Paid' else {ENGAGE_DISPOSITION_COL} end,
                    {PAYMENT_LOG_STATUS_COL} = case when new_paid_amount = %s then True else {PAYMENT_LOG_STATUS_COL} end,
                    payment_amount = case when payment_amount is Null then new_paid_amount else payment_amount + new_paid_amount end,
                    %s = %s - new_paid_amount
                    WHERE ({ENGAGE_DISPOSITION_COL} in ('', 'CLOSED', 'STOP_FOLLOWUP') OR {ENGAGE_DISPOSITION_COL} is NULL) AND
                    job_id = '%s' ''' % (amount_col, amount_col, amount_col, amount_col, self.job_id)
        return query_string

    def process_complete_payment(self, sales_order_number: int, account_numbers: List[int]) -> bool:
        mysql_conn = MySQL()
        read_options = {
            "WHERE": [
                {
                    ENGAGE_LOAN_ACC_COL: account_numbers,
                    ENGAGE_DISPOSITION_COL: [None, "", "CLOSED", "STOP_FOLLOWUP"],
                    "job_id": self.job_id
                }
            ]
        }
        entries = mysql_conn.fetch_entries(ENGAGE_TABLE_NAME, read_options, columns=[ENGAGE_LOAN_ACC_COL, "sp_amount"])
        data = entries['data']

        if len(data) == 0:
            return True

        payment_log_data = []
        final_account_numbers = []
        for row in data:
            payment_log = {
                PAYMENT_LOG_STATUS_COL: True,
                "sales_order_no": sales_order_number,
                ENGAGE_LOAN_ACC_COL: row[0],
                "total_outstanding": row[1],
                "current_outstanding": 0,
                "payment_amount": row[1],
                ENGAGE_DISPOSITION_COL: "Paid"
            }
            payment_log_data.append(payment_log)
            final_account_numbers.append(row[0])

        insert_options = {
            "data": payment_log_data
        }

        update_options = {
            "data": {
                PAYMENT_LOG_STATUS_COL: True,
                ENGAGE_DISPOSITION_COL: "Paid"
            },
            "WHERE": [
                {
                    ENGAGE_LOAN_ACC_COL: final_account_numbers
                }
            ]
        }

        done = mysql_conn.update_entries(ENGAGE_TABLE_NAME, update_options, commit_result=False)

        if done is False:
            return False

        return mysql_conn.create_entries(PAYMENT_LOG_TABLE_NAME, insert_options,
                                         [PAYMENT_LOG_STATUS_COL, "sales_order_no", ENGAGE_LOAN_ACC_COL, "total_outstanding",
                                          "current_outstanding", "payment_amount", ENGAGE_DISPOSITION_COL])

    def process_payment_file(self, user_mapping: Dict[str, str], file_name: str) -> bool:
        df = read_file(file_name, folder_type=PAYMENT_FOLDER)
        df = map_column_headers(map_column_headers(df, user_mapping), PAYMENT_FILE_MAPPING)
        job = Job()
        sales_order_number = job.fetch_job_detail(id=self.job_id, param='sales_order_no')

        if ENGAGE_DISPOSITION_COL in df.columns:
            paid_accounts_df = df[df[ENGAGE_DISPOSITION_COL].str.lower().str.strip().isin(PAYMENT_FILE_PAID_STATUS)]

            if paid_accounts_df.shape[0] == 0:
                return True

            paid_account_numbers = paid_accounts_df[ENGAGE_LOAN_ACC_COL].to_list()

            account_chunks = create_chunks(paid_account_numbers, CHUNK_SIZE)

            for chunk in account_chunks:
                done = self.process_complete_payment(sales_order_number, chunk)
                if done is False:
                    return False
            return True
        else:
            number_of_splits = math.ceil(df.shape[0] / CHUNK_SIZE)
            df_chunks = np.array_split(df, number_of_splits)

            for chunk in df_chunks:
                done = self.process_partial_payment(sales_order_number, chunk)
                if done is False:
                    return False
            return True
