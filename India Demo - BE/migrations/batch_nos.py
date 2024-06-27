"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import logging

import pandas as pd
import sqlalchemy
from tqdm import tqdm

from config.config import ENGAGE_DBConfig
from schemas.PostgreSQL import PostgreSQL
from utils.functions import common_between_lists, remove_duplicates_from_list

# logging.basicConfig(filename='../logs/batch_nos.log', encoding='utf-8', level=logging.DEBUG,
#                     format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', datefmt='%Y-%m-%d %H:%M:%S')

pg = PostgreSQL()

TABLE_NAME = 'data_organize.file_details'
columns = ['batch', 'created_date']
options = {'WHERE': [{'customerid': '835'}, {'batch': '210'}, {'customer_type': 'spocto_trace'}]}

data = pg.fetch_entries(table_name=TABLE_NAME, options=options, columns=columns)
df = pd.DataFrame(data['data'], columns=columns)

df.sort_values(by=['created_date'], inplace=True, ascending=False)
df.drop_duplicates(subset=['batch'], keep='first', inplace=True)

print(len(df))

print(df.head())

print('Initializing...')

PAGE_SIZE = 1000

db1 = sqlalchemy.create_engine(f'mysql+{ENGAGE_DBConfig.DRIVER}://{ENGAGE_DBConfig.USERNAME}:{ENGAGE_DBConfig.PASSWORD}@sp835.spoctocular.com:{ENGAGE_DBConfig.PORT}/{ENGAGE_DBConfig.DB_NAME}')
db2 = sqlalchemy.create_engine(f'mysql+{ENGAGE_DBConfig.DRIVER}://{ENGAGE_DBConfig.USERNAME}:{ENGAGE_DBConfig.PASSWORD}@{ENGAGE_DBConfig.HOST}:{ENGAGE_DBConfig.PORT}/{ENGAGE_DBConfig.DB_NAME}')

print('Writing...')

SPOCTO_TRACE_COLUMNS = [
    'id', 'owner_id', 'stage_id', 'is_published', 'date_added', 'created_by',
    'created_by_user',
    'date_modified', 'modified_by', 'modified_by_user', 'checked_out', 'checked_out_by',
    'checked_out_by_user', 'points', 'last_active', 'internal', 'social_cache',
    'date_identified',
    'preferred_profile_image', 'title', 'firstname', 'lastname', 'company', 'position', 'email',
    'phone', 'mobile', 'address1', 'address2', 'city', 'state', 'zipcode', 'timezone',
    'country',
    'fax', 'preferred_locale', 'attribution_date', 'attribution', 'website', 'facebook',
    'foursquare', 'googleplus', 'instagram', 'linkedin', 'skype', 'twitter', 'dpd', 'email_2',
    'email_3', 'email_4', 'sp_call_answered', 'sp_call_pickup_time', 'sp_call_hangup_time',
    'sp_call_duration', 'sp_call_action', 'sp_call_location', 'sp_message_status', 'mobile_2',
    'sp_call_answered_2', 'sp_call_pickup_time_2', 'sp_call_hangup_time_2',
    'sp_call_duration_2',
    'sp_call_action_2', 'sp_call_location_2', 'sp_message_status_2', 'mobile_3',
    'sp_call_answered_3', 'sp_call_pickup_time_3', 'sp_call_hangup_time_3',
    'sp_call_duration_3',
    'sp_call_action_3', 'sp_call_location_3', 'sp_message_status_3', 'mobile_4',
    'sp_call_answered_4', 'sp_call_pickup_time_4', 'sp_call_hangup_time_4',
    'sp_call_duration_4',
    'sp_call_action_4', 'sp_call_location_4', 'sp_message_status_4', 'amount_due', 'sp_date',
    'sp_principal_outstanding', 'customerid', 'is_paid', 'payment_date', 'payment_amount',
    'sales_order_no', 'sp_amount', 'language', 'channel_of_payment', 'mode_of_payment',
    'disposition', 'overall_result_type', 'is_ndnc', 'is_ndnc2', 'is_ndnc3', 'is_ndnc4',
    'masked_loan_account_no', 'sp_account_number', 'sp_product', 'spoctoid',
    'minimum_amount_due',
    'last_followup_date', 'company_level1_contact_na', 'company_level1_contact_mo',
    'payment_link',
    'total_overdue', 'credit_score', 'loan_score', 'social_score', 'mmp_reacted', 'due_date',
    'recommendation1', 'recommendation2', 'other_payment_link', 'flex_1', 'flex_2', 'flex_3',
    'flex_4', 'flex_5', 'flex_6', 'flex_7', 'flex_8', 'flex_9', 'flex_10', 'flex_11',
    'masked_mobile', 'job_id', 'is_link_valid', 'original_amount', 'repeated_final_status'
]
SP_LEADS_COLUMNS = [
    'address1', 'address2', 'amount_due', 'attribution', 'attribution_date', 'batch_no', 'channel_of_payment',
    'checked_out', 'checked_out_by', 'checked_out_by_user', 'city', 'company', 'company_level1_contact_mo',
    'company_level1_contact_na', 'country', 'created_by', 'created_by_user', 'credit_score', 'customerid',
    'date_added',
    'date_identified', 'date_modified', 'disposition', 'dpd', 'due_date', 'email', 'email_2', 'email_3', 'email_4',
    'facebook', 'fax', 'firstname', 'flex_1', 'flex_10', 'flex_11', 'flex_2', 'flex_3', 'flex_4', 'flex_5',
    'flex_6',
    'flex_7', 'flex_8', 'flex_9', 'foursquare', 'googleplus', 'id', 'instagram', 'internal', 'is_ndnc', 'is_ndnc2',
    'is_ndnc3', 'is_ndnc4', 'is_paid', 'is_published', 'language', 'last_active', 'last_followup_date', 'lastname',
    'linkedin', 'loan_score', 'masked_loan_account_no', 'minimum_amount_due', 'mmp_reacted', 'mobile', 'mobile_2',
    'mobile_3', 'mobile_4', 'mode_of_payment', 'modified_by', 'modified_by_user', 'other_payment_link',
    'overall_result_type', 'owner_id', 'payment_amount', 'payment_date', 'payment_link', 'phone', 'points',
    'position',
    'preferred_locale', 'preferred_profile_image', 'recommendation1', 'recommendation2', 'skype', 'social_cache',
    'social_score', 'sp_account_number', 'sp_amount', 'sp_call_action', 'sp_call_action_2', 'sp_call_action_3',
    'sp_call_action_4', 'sp_call_answered', 'sp_call_answered_2', 'sp_call_answered_3', 'sp_call_answered_4',
    'sp_call_duration', 'sp_call_duration_2', 'sp_call_duration_3', 'sp_call_duration_4', 'sp_call_hangup_time',
    'sp_call_hangup_time_2', 'sp_call_hangup_time_3', 'sp_call_hangup_time_4', 'sp_call_location',
    'sp_call_location_2',
    'sp_call_location_3', 'sp_call_location_4', 'sp_call_pickup_time', 'sp_call_pickup_time_2',
    'sp_call_pickup_time_3',
    'sp_call_pickup_time_4', 'sp_date', 'sp_message_status', 'sp_message_status_2', 'sp_message_status_3',
    'sp_message_status_4', 'sp_principal_outstanding', 'sp_product', 'spoctoid', 'stage_id', 'state', 'timezone',
    'title', 'total_overdue', 'twitter', 'website', 'zipcode'
]

COMMON_COLUMNS = common_between_lists(SPOCTO_TRACE_COLUMNS, SP_LEADS_COLUMNS)
COMMON_COLUMNS = remove_duplicates_from_list(COMMON_COLUMNS)


def fetch_and_move_data_to_trace(row):
    try:
        count_query = f'''SELECT count(*) FROM sp_leads where batch_no in ({row["batch"]})'''
        print(f'Count Query 1: {count_query}')
        result = db1.execute(count_query)
        print(f'Count Query 2: {result}')
        total_count = 0

        for r in result:
            total_count = r[0]

        print(f'Count Query 3: {total_count}')

        string_spocto_trace_cols = ', '.join(map(str, COMMON_COLUMNS))

        for i in range(0, total_count, PAGE_SIZE):
            print(f'Loop: {i} -> Batch No: {row["batch"]}')
            query = f'''SELECT {string_spocto_trace_cols} FROM sp_leads WHERE batch_no = {row["batch"]} LIMIT {PAGE_SIZE} OFFSET {i}'''
            df_sp_leads = pd.read_sql(query, db1)
            df_sp_leads["date_added"] = row["created_date"]
            df_sp_leads.to_sql('spocto_trace', db2, if_exists='append', index=False)
        return 200
    except Exception as e:
        logging.error(f'Error: {row["batch"]} -> {e}')
        return 400


for index, row in tqdm(df.iterrows()):
    status = fetch_and_move_data_to_trace(row=row)
    if status == 200:
        logging.info(f'Finished -> Index: {index}: {row["batch"]}')
    elif status == 400:
        logging.info(f'BATCH -> Index: {index}: {row["batch"]}')
        continue
