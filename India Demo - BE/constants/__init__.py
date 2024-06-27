"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

ALLOWED_EXTENSIONS = {'csv', 'xlsx', 'xls', 'xlsb', 'pkl'}
FILE_EXTENSIONS = {'CSV': 'csv', 'Excel': 'xlsx'}
CSV_ALLOWED_TYPES = {'.csv'}
EXCEL_ALLOWED_TYPES = {'.xlsx', '.xls', '.xlsb'}
DATATYPE_MAPPING = {
    'mobile': 'Mobile',
    'location': 'Location',
    'email': 'Email',
    'loan_account_no': 'Account Number',
    'datetime': 'DateTime',
    'name': 'Name',
    'product': 'Product',
    'pincode': 'Pincode',
    'amount': 'Amount',
    'boolean': 'Boolean',
    'float': 'Float',
    'number': 'Number',
    'derived_status': 'Derived Status',
    'geo_code': 'Geo Code'
}
FUNCTION_MAPPING = {
    'Mobile': 'check_mobile',
    'Location': 'check_text',
    'Email': 'check_email',
    'Account Number': 'check_text',  # check_alphanumeric_no_space
    'DateTime': 'check_date',
    'Name': 'check_name',
    'Product': 'check_text',
    'Number': 'check_number',
    'Pincode': 'check_number',
    'Amount': 'check_amount',
    'Text': 'check_text',
    'Boolean': 'check_bool',
    'Float': 'check_float',
    'Derived Status': 'check_derived_status',
    'Geo Code': 'check_geo_code'
}
DTYPE_MAPPING = {
    'object': 'Text',
    'int64': 'Number',
    'float64': 'Float',
    'datetime64[ns]': 'DateTime',
    'bool': 'Boolean'
}
TEXT_LIKE_DATATYPES = ['Mobile', 'Location', 'Email', 'Account Number', 'Name', 'Product', 'Text', 'Derived Status']
NUMBER_LIKE_DATATYPES = ['Number', 'Pincode', 'Amount', 'Float']
ORDER = {'asc': 1, 'desc': -1}
ORIENTATION = {'first': 1, 'last': -1}
ENGAGE_TABLE_COLUMNS = {
    'Address Country': 'country',
    'Address City': 'city',
    'Address State': 'state',
    'Address Zipcode': 'zipcode',
    'Billing Cycle': 'due_date',
    'Country Code': 'flex_10',
    'CustomerID': 'customerid',
    'Customer Email': 'email',
    'Customer Email 2': 'email_2',
    'Customer First Name': 'firstname',
    'Customer Last Name': 'lastname',
    'Customer Mobile': 'mobile',
    'Customer Mobile 2': 'mobile_2',
    'Date Of Birth': 'flex_11',
    'Days Past Due (DPD)': 'dpd',
    'EMI Amount': 'amount_due',
    'Flex 1': 'flex_1',
    'Flex 2': 'flex_2',
    'Flex 3': 'flex_3',
    'Flex 4': 'flex_4',
    'Flex 5': 'flex_5',
    'Flex 6': 'flex_6',
    'Fresh - Repeat Status': 'repeated_final_status',
    'Gender': 'flex_7',
    'Last Followup Date': 'last_followup_date',
    'Last Payment Date': 'sp_date',
    'Loan Account No': 'sp_account_number',
    'Masked Loan Account No': 'masked_loan_account_no',
    'Minimum Amount Due': 'minimum_amount_due',
    'Nationality': 'flex_8',
    'Other Payment Link': 'other_payment_link',
    'Overdue Charges': 'total_overdue',
    'Payment Link': 'payment_link',
    'Principal Outstanding Amount (POS)': 'sp_principal_outstanding',
    'Product Type': 'sp_product',
    'Propensity To Pay': 'recommendation1',
    'Repeat Frequency': 'flex_9',
    'SMART Ranking Score': 'recommendation2',
    'Total Outstanding Amount (TOS)': 'sp_amount',
    'Title': 'title'
}
ENGAGE_COLUMNS_WITH_UNIQUE_VALUES = [
    'customerid', 'email', 'email_2', 'firstname', 'lastname', 'mobile', 'mobile_2', 'amount_due',
    'sp_account_number', 'masked_loan_account_no', 'minimum_amount_due', 'total_overdue',
    'sp_principal_outstanding', 'sp_amount', 'due_date', 'flex_11', 'last_followup_date', 'sp_date'
]
PAYMENT_FILE_PAID_STATUS = ['paid', 'resolved', 'full', 'clr']
ALLOCATION_FOLDER = 'ALLOCATION_FOLDER'
PAYMENT_FOLDER = 'PAYMENT_FOLDER'
INVALID_FOLDER = 'INVALID_FOLDER'
MASKED_STRING_LENGTH = 'MASKED_STRING_LENGTH'
PAYMENT_FILE_MAPPING = {
    'EMI Amount': 'amount_due',
    'Loan Account Number': 'sp_account_number',
    'Minimum Amount Due': 'minimum_amount_due',
    'Overdue Charges': 'total_overdue',
    'Payment Amount': 'payment_amount',
    'Payment Status': 'disposition',
    'Principal Outstanding Amount (POS)': 'sp_principal_outstanding',
    'Total Outstanding Amount (TOS)': 'sp_amount'
}
PAYMENT_FILE_AMOUNT_COLUMNS = [
    'amount_due', 'minimum_amount_due', 'total_overdue', 'payment_amount',
    'sp_principal_outstanding', 'sp_amount'
]
TASK_STATUS_SUCCESS = 'success'
TASK_STATUS_FAIL = 'fail'
TASK_STATUS_CREATED = 'created'
TASK_STATUS_CLIENT_ERROR = 'client_error'
DERIVED_VALID_MOBILE_COL = 'DERIVED_VALID_MOBILE'
DERIVED_VALID_EMAIL_COL = 'DERIVED_VALID_EMAIL'
ENGAGE_LOAN_ACC_COL = 'sp_account_number'
ENGAGE_DISPOSITION_COL = 'disposition'
ENGAGE_LOAN_ACC_FREQ_COL = 'ACC_NO_FREQ'
ENGAGE_PAID_FREQ_COL = 'PAID_FREQ'
FRESH_REPEAT_STATUS_COL = 'FRESH_REPEAT_STATUS'
REPEAT_FREQUENCY_COL = 'REPEAT_FREQUENCY'
BOUNCE_RATE_COL = 'BOUNCE_RATE'
PAYMENT_LOG_STATUS_COL = 'is_paid'
PAYMENT_LOG_CREATED_DATE_COL = 'created_date'
SPOCTO_TRACE_CREATED_DATE_COL = 'date_added'
L3M_REPEATED_BOUNCED_STATUS = 'Repeated & Bounced'
L3M_REPEATED_CLEARED_STATUS = 'Repeated & Cleared'
VALID_STATUS_COL = 'is_valid'
COL_MAPPING_COL = 'column_mapping'
REV_COLUMN_MAPPING_COL = 'rev_column_mapping'
L3M_FRESH_STATUS = 'Fresh'
ENGAGE_TABLE_NAME = 'sp_leads'
SPOCTO_TRACE_TABLE_NAME = 'spocto_trace'
PAYMENT_LOG_TABLE_NAME = 'sp_payment_log'
PERIOD_FOR_HISTORICAL_DATA = 6  # Denotes number of months
RESERVED_COLUMN_NAMES = [DERIVED_VALID_MOBILE_COL, DERIVED_VALID_EMAIL_COL, FRESH_REPEAT_STATUS_COL, BOUNCE_RATE_COL]
RESERVED_KEYWORDS = ['range', 'percent', 'ratioed']
INVALID_INPUT_ERROR_STATUS = 'INVALID_INPUT_ERROR'
SPOCTO_INDIAN_STATE = 'SPOCTO_INDIAN_STATE'
MENU_OPTIONS_SEARCH = 'search'
MENU_OPTIONS_SORT = 'sort'
MENU_OPTIONS_FILTER = 'filter'
MENU_OPTIONS_SPLIT = 'split'
MENU_OPTIONS_MERGE = 'merge'
MENU_OPTIONS_DELETE_COLUMN = 'delete_column'
MENU_OPTIONS_RENAME = 'rename'
MENU_OPTIONS_CALCULATE_WAIVER = 'waiver'
MENU_OPTIONS_MASK = 'mask'
MENU_OPTIONS_RANGE = 'range'
MENU_OPTIONS_RATIO = 'ratio'
MENU_OPTIONS_ADD_STATIC_VALUE = 'add_static_value'
MENU_OPTIONS_ADD_DYNAMIC_MAPPING = 'add_dynamic_mapping'
MENU_OPTIONS_STATE_MAPPING = 'state_mapping'
MENU_OPTIONS_ROUND = 'round'
MENU_OPTIONS_ROUND_UP = 'round_up'
MENU_OPTIONS_ROUND_DOWN = 'round_down'
MENU_OPTIONS_ROUND_OFF = 'round_off'
MENU_OPTIONS_NUMBER_OF_YEARS = 'num_of_years'
MENU_OPTIONS_AGE = 'age'
MENU_OPTIONS_ACTION_DETAILS = {
    MENU_OPTIONS_SPLIT: 'Split',
    MENU_OPTIONS_MERGE: 'Merge',
    MENU_OPTIONS_DELETE_COLUMN: 'Delete Column',
    MENU_OPTIONS_RENAME: 'Rename',
    MENU_OPTIONS_CALCULATE_WAIVER: 'Waiver Value Calculation',
    MENU_OPTIONS_MASK: 'Mask',
    MENU_OPTIONS_RANGE: 'Range',
    MENU_OPTIONS_RATIO: 'Ratio',
    MENU_OPTIONS_ADD_STATIC_VALUE: 'Add Column - Static Value',
    MENU_OPTIONS_ADD_DYNAMIC_MAPPING: 'Add Column - Dynamic Mapping',
    MENU_OPTIONS_STATE_MAPPING: 'State Mapping',
    MENU_OPTIONS_ROUND: 'Rounding Operation',
    MENU_OPTIONS_NUMBER_OF_YEARS: 'Number Of Years'
}
SUMMARY_PROPERTY_UNIQUE = 'unique'
SUMMARY_PROPERTY_MAX_MIN = 'max_min'
SUMMARY_PROPERTY_MAX = 'max'
SUMMARY_PROPERTY_MIN = 'min'
STATE = 'state'
SYSTEM_DATETIME_FORMAT = '%Y-%m-%d %H:%M:%S'
