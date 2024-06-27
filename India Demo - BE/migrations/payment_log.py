"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import pandas as pd
import sqlalchemy

from config.config import ENGAGE_DBConfig

PAGE_SIZE = 1000

db1 = sqlalchemy.create_engine(
    f'mysql+{ENGAGE_DBConfig.DRIVER}://{ENGAGE_DBConfig.USERNAME}:{ENGAGE_DBConfig.PASSWORD}@sp835.spoctocular.com:{ENGAGE_DBConfig.PORT}/{ENGAGE_DBConfig.DB_NAME}')
db2 = sqlalchemy.create_engine(
    f'mysql+{ENGAGE_DBConfig.DRIVER}://{ENGAGE_DBConfig.USERNAME}:{ENGAGE_DBConfig.PASSWORD}@{ENGAGE_DBConfig.HOST}:{ENGAGE_DBConfig.PORT}/{ENGAGE_DBConfig.DB_NAME}')

print('Writing...')

count_query = '''SELECT count(*) FROM sp_payment_log WHERE created_date >= now() - INTERVAL 6 MONTH '''

result = db1.execute(count_query)

print(f'Count: {result}')

COMMON_COLUMNS = ['id', 'sp_account_number', 'total_outstanding', 'current_outstanding', 'mode_of_payment', 'payment_amount', 'payment_date', 'disposition', 'is_paid', 'created_date', 'payment_month']

string_spocto_trace_cols = ', '.join(map(str, COMMON_COLUMNS))

total_count = 0

for r in result:
    total_count = r[0]

print(f"Total Count: {total_count}")

for i in range(0, total_count, PAGE_SIZE):
    print(f'Loop: {i}')
    query = f'''SELECT {string_spocto_trace_cols} from sp_payment_log WHERE created_date >= now() - INTERVAL 6 MONTH LIMIT {PAGE_SIZE} OFFSET {i}'''
    df = pd.read_sql(query, db1)
    df.to_sql('sp_payment_log', db2, if_exists='append', index=False)

print('sp_leads copied.')
