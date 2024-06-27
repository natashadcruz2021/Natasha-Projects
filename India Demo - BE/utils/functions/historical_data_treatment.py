"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import logging
from typing import Dict

import numpy as np
import pandas as pd
from pandas import DataFrame

from config.config import ENGAGE_DBConfig
from constants import ENGAGE_LOAN_ACC_COL, ENGAGE_LOAN_ACC_FREQ_COL, ENGAGE_PAID_FREQ_COL, PAYMENT_LOG_STATUS_COL, \
    L3M_REPEATED_BOUNCED_STATUS, L3M_REPEATED_CLEARED_STATUS, L3M_FRESH_STATUS, SPOCTO_TRACE_TABLE_NAME, \
    PAYMENT_LOG_TABLE_NAME, PERIOD_FOR_HISTORICAL_DATA, PAYMENT_LOG_CREATED_DATE_COL, FRESH_REPEAT_STATUS_COL, \
    BOUNCE_RATE_COL, REPEAT_FREQUENCY_COL, SPOCTO_TRACE_CREATED_DATE_COL
from schemas.MySQL import MySQL
from utils.classes.CustomErrors import InvalidInputError


def last_three_month_data_load(df: DataFrame, header_mapping_datatype: Dict[str, str]) -> DataFrame:
    """
      - Checking in spocto_trace to see which all loan_account_nos are there from earlier
      - As we don't know which column could be loan_account_number, we check from the
        datatype mapping for the key which has the value "Account Number" - this has been
        blocked by the FE to be never more than 1 of, in this datatype mapping.
    """
    account_col_name = ''
    account_number_datatype = []
    for k, v in header_mapping_datatype.items():
        if v == 'Account Number':
            account_number_datatype.append(k)
            account_col_name = k
    """
      - The conversion below is done to handle cases like in IDFC where loan_account_no tends to be pure numeric and causes
        an issue where the loan_account_col has not been validated yet and so Pandas infers the datatype as "int64" and the 
        datatype in the databases for the same column is "object" (or "string"). And hence, merge breaks because you cannot
        merge 2 columns with different datatypes.
    """
    df[account_col_name] = df[account_col_name].astype(str)
    # Check put on FE to not have more than 1 columns with "Account Number" as datatype. Handling in BE as well, just in case.
    if len(account_number_datatype) > 1:
        raise InvalidInputError(f'These following columns have the "Account Number" datatype: {", ".join(map(str, account_number_datatype))}')
    if account_col_name == '':
        logging.error('Could not compute L3M data as "Account Number" does not exist.')
        return df
    """
      - Having spoken to the Ops team, almost every time the file will have unique loan account no.
      - In case, that is not the case, remove any row, without consideration.
      - The check for removal of "None" happens because at times, a file may receive every column as blank
        except the payment link columns. So, they will be flagged as invalid in the invalid check process.
    """
    loan_account_no_list = list(set(list(df[account_col_name])))
    if None in loan_account_no_list:
        loan_account_no_list.remove(None)
    engage_sql = MySQL(host=ENGAGE_DBConfig.HOST, username=ENGAGE_DBConfig.USERNAME, port=ENGAGE_DBConfig.PORT, db=ENGAGE_DBConfig.DB_NAME, password=ENGAGE_DBConfig.PASSWORD)
    trace_data = engage_sql.fetch_entries(table_name=SPOCTO_TRACE_TABLE_NAME, options={'WHERE': [{ENGAGE_LOAN_ACC_COL: loan_account_no_list}, {SPOCTO_TRACE_CREATED_DATE_COL: ('>=', f'now() - INTERVAL {PERIOD_FOR_HISTORICAL_DATA} MONTH')}], 'GROUP BY': ENGAGE_LOAN_ACC_COL}, columns=[ENGAGE_LOAN_ACC_COL, f'COUNT(*) AS {ENGAGE_LOAN_ACC_FREQ_COL}'])
    df_trace = pd.DataFrame(trace_data['data'], columns=[ENGAGE_LOAN_ACC_COL, ENGAGE_LOAN_ACC_FREQ_COL])
    df_trace.drop_duplicates(subset=[ENGAGE_LOAN_ACC_COL], inplace=True)
    trace_loan_account_no_list = list(df_trace[ENGAGE_LOAN_ACC_COL])
    """
    Checking for CLEAR or BOUNCE - in the payment data
    """
    if len(df_trace) != 0:
        engage_data = engage_sql.fetch_entries(table_name=PAYMENT_LOG_TABLE_NAME, options={'WHERE': [{ENGAGE_LOAN_ACC_COL: trace_loan_account_no_list}, {PAYMENT_LOG_CREATED_DATE_COL: ('>=', f'now() - INTERVAL {PERIOD_FOR_HISTORICAL_DATA} MONTH')}], 'ORDER BY': PAYMENT_LOG_STATUS_COL}, columns=[ENGAGE_LOAN_ACC_COL, PAYMENT_LOG_STATUS_COL, PAYMENT_LOG_CREATED_DATE_COL])
        df_payment = pd.DataFrame(engage_data['data'], columns=[ENGAGE_LOAN_ACC_COL, PAYMENT_LOG_STATUS_COL, PAYMENT_LOG_CREATED_DATE_COL])
        if len(df_payment) != 0:
            """
              - Grouping by 'ENGAGE_LOAN_ACC_COL', 'PAYMENT_LOG_STATUS_COL' to get 'ENGAGE_PAID_FREQ_COL'.
              - This was done at a df level and not at a SQL level so as to get the 'PAYMENT_LOG_CREATED_DATE_COL'
                and get the 'ENGAGE_PAID_FREQ_COL' as well without querying the DB twice.
              - df_payment_grouped is created to get 'ENGAGE_PAID_FREQ_COL'.
              - df_payment is the df that will initially, keep the values returned from sp_payment_log and finally,
                keep the 'ENGAGE_PAID_FREQ_COL' tagged with the 'PAYMENT_LOG_STATUS_COL' based on the latest value
                in 'PAYMENT_LOG_CREATED_DATE_COL'.
              - Merging the two df to get "is_paid" status of the latest record while keeping the
                'ENGAGE_PAID_FREQ_COL' of all the previous records.
              - Sorting df_payment first (by datetime to ensure everything other than the latest gets removed) 
                and then dropping duplicates before merging so as to remove all the entries except for the latest one
                as only that entry's status is needed when the 2 dfs will be merged.
              - On merging the 2 dfs, "is_paid_x" and "is_paid_y" columns are created. Deleting the "is_paid_y"
                as it is from df_payment_grouped. Renaming "is_paid_x" to 'PAYMENT_LOG_STATUS_COL' as it is the
                latest status value for that particular loan account no.
              - If "is_paid" is 0 -> "Repeated & Bounced", else "Repeated & Cleared".
            """
            df_payment_grouped = df_payment[df_payment[PAYMENT_LOG_STATUS_COL] == 1].groupby(by=[ENGAGE_LOAN_ACC_COL, PAYMENT_LOG_STATUS_COL]).count().reset_index()
            df_payment_grouped.rename(columns={PAYMENT_LOG_CREATED_DATE_COL: ENGAGE_PAID_FREQ_COL}, inplace=True)
            df_payment.sort_values(by=[PAYMENT_LOG_CREATED_DATE_COL], inplace=True, ascending=False)
            df_payment.drop_duplicates(subset=[ENGAGE_LOAN_ACC_COL], inplace=True)
            df_payment = df_payment.merge(df_payment_grouped, on=[ENGAGE_LOAN_ACC_COL], how='left', indicator=True)
            del df_payment_grouped
            df_payment.drop(['is_paid_y', PAYMENT_LOG_CREATED_DATE_COL, '_merge'], axis=1, inplace=True)
            df_payment.rename(columns={'is_paid_x': PAYMENT_LOG_STATUS_COL}, inplace=True)
            df_payment = df_trace.merge(df_payment, on=[ENGAGE_LOAN_ACC_COL], how='left', indicator=True)
            df_payment[ENGAGE_PAID_FREQ_COL].replace({np.nan: 0}, inplace=True)
            del df_trace
            df_payment[BOUNCE_RATE_COL] = df_payment[ENGAGE_LOAN_ACC_FREQ_COL] - df_payment[ENGAGE_PAID_FREQ_COL]
            df_payment[PAYMENT_LOG_STATUS_COL].replace({np.nan: 0}, inplace=True)
            df_payment[FRESH_REPEAT_STATUS_COL] = np.where(df_payment[PAYMENT_LOG_STATUS_COL] == 1, L3M_REPEATED_CLEARED_STATUS, L3M_REPEATED_BOUNCED_STATUS)
            df_payment.drop(columns=['_merge'], inplace=True)
        else:
            df_trace[FRESH_REPEAT_STATUS_COL] = L3M_REPEATED_BOUNCED_STATUS
            df_trace[BOUNCE_RATE_COL] = df_trace[ENGAGE_LOAN_ACC_FREQ_COL]
            df_payment = df_trace
            del df_trace
        df = df.merge(df_payment, left_on=[account_col_name], right_on=[ENGAGE_LOAN_ACC_COL], how='left', indicator=True)
        del df_payment
        df[FRESH_REPEAT_STATUS_COL] = np.where(df['_merge'] == 'left_only', L3M_FRESH_STATUS, df[FRESH_REPEAT_STATUS_COL])
        columns_to_be_dropped = ['_merge']
        """
        - Dropping column for variable 'ENGAGE_PAID_FREQ_COL' as 'BOUNCE_RATE_COL' is a column
          that will be duplicate of this.
        - Dropping column for variable 'PAYMENT_LOG_STATUS_COL' as 'PAYMENT_LOG_STATUS_COL' is used to
          to mark 'L3M_REPEATED_BOUNCED_STATUS' or 'L3M_REPEATED_CLEARED_STATUS'.
        """
        if ENGAGE_LOAN_ACC_COL in list(df.columns):
            columns_to_be_dropped.append(ENGAGE_LOAN_ACC_COL)
        if ENGAGE_PAID_FREQ_COL in list(df.columns):
            columns_to_be_dropped.append(ENGAGE_PAID_FREQ_COL)
        if PAYMENT_LOG_STATUS_COL in list(df.columns):
            columns_to_be_dropped.append(PAYMENT_LOG_STATUS_COL)
        df.drop(columns=columns_to_be_dropped, inplace=True)
        df.rename(columns={ENGAGE_LOAN_ACC_FREQ_COL: REPEAT_FREQUENCY_COL}, inplace=True)
    else:
        df[FRESH_REPEAT_STATUS_COL] = L3M_FRESH_STATUS
        df[REPEAT_FREQUENCY_COL] = 0
        df[BOUNCE_RATE_COL] = 0
    """
      - If in a column, there are values with decimal points i.e. 9.0, instead of considering it as float, the check for 
        "Valid" / "Invalid" considers it as string. Hence, it was being marked as "Invalid".
      - Hence, REPEAT_FREQUENCY_COL & BOUNCE_RATE_COL are being converted to INTEGER, as being system generated columns,
        they are considered as columns with datatype "Number".
      - While conversion, if "NaN" received, pandas breaks. Hence, filling "NaN" values with 0.
    """
    df = df.fillna(value={REPEAT_FREQUENCY_COL: 0, BOUNCE_RATE_COL: 0})
    df = df.astype({REPEAT_FREQUENCY_COL: int, BOUNCE_RATE_COL: int})
    df[account_col_name].replace({f'None': None}, inplace=True)
    return df
