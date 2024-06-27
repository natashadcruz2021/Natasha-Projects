## Data Cleaning & Validation
---
- **Identifying headers** <small>Function [ Input: DataFrame, Output: Dictionary ]</small>
  - Determining the datatype for header based on historical datatype column mapping maintained in "historical_column_mapping.json".
  - If there is no mapping present in historical data then datatype is identified using top 10 records present in DataFrame.
  - If there is no data found in DataFrame then that header's datatype is by default marked as Text.
  - Following is sample Dictionary returned by the function:
    ```js
    {
        'PORTFOLIO': 'Product',
        'BRANCH_NAME': 'Text',
        'REGION': 'Location',
        'BRANCH_CODE': 'Number',
        'PROPNO': 'Text',
        'AGMTNO': 'Text',
        'RISK': 'Text',
        'CUSTOMER_NAME': 'Name',
        'AGMT_DATE': 'DateTime',
        'STATUS': 'Amount',
        'REPAYMENT_MODE': 'Text',
        'FIRST_EMI_DATE': 'DateTime',
        'CYCLE_DATE': 'Number',
        'CURRENT_DEMAND_AMOUNT': 'Number',
        'OPENING_BUCKET': 'Number',
        'MOBILE1': 'Mobile',
        'MODEL_DESC': 'Text',
        'BOUNCE_PROPENSITY': 'Text',
        'DECILE': 'Number',
        'MOB': 'Mobile',
        'CURRENT_MONTH_STATUS': 'Text',
        'LOAN_AMOUNT': 'Amount',
        'TYPE_OF_CALLING': 'Text',
        'TENUR': 'Number',
        'ADVANCE_EMI_COUNT': 'Number',
        'FINTECH_VENDOR': 'Text',
        'CIBIL_SCORE': 'Number',
        'PROFILE_DESC': 'Text',
        'PINCODE': 'Number'
    }
    ```

- **Validating Columns** <small>Function [ Input: Data {DataFrame}, Column Mapping {Dictionary}, Output: List {Dictionary} ]</small>
  - In this function entire Data is taken in form of DataFrame along with each columns datatype for validating it's records.
  - The DataFrame is then converted into Dictionary and each row is iterated and based on column header's datatype, functions are called which are maintained in following mapping.

  ```js
    {
        'Mobile': 'check_mobile',
        'Location': 'check_geo',
        'Email': 'check_email',
        'Account Number': 'check_alphanumeric_no_space',
        'DateTime': 'check_date',
        'Name': 'check_name',
        'Product': 'check_product',
        'Pincode': 'check_number',
        'Amount': 'check_amount',
        'Text': 'check_alphanumeric',
    }
  ```
    
    - Following is explaination of what happens inside each function:
      * **Check Mobile:** 
            - It splits the string with following delimiters ( ; | , | _ | - )
            - And then loops over the list and then following operation are performed on each records: strip spaces from beginning and end, strip '0's from left, split with decimal if present and store it's zeroth index data, remove all characters except numbers.
            - If length of string is greater than 10 then strip '91' and '0' from left of the string if present.
            - if length of the string is equal to 10 then it marked as valid else it is marked as invalid.
            - The results are stored in separate list as per valid and invalid and on returning the results the lists are merged with valid mobile numbers list being at first place and then invalid mobile numbers. 
  
      * **Check Geo:**
            - The string is first made sentence cased and then spaces are stripped from start and end of the string.
            - Then using Spacy's Entity Ruler few patterns are added in entity ruler pipe and new config file is generated, the patterns are stored in location.json file and the config file generated is stored in folder.
            - Based on this stored config file the NER pipeline is initalized and values are passed through this pipeline and the output returned contains "LOC" is checked.
            - If the output contains "LOC" then its valid else invalid.
            - If one wants to add new patterns for recognition then it can be added in location.json file (keep sentence casing).

      * **Check Email:**
            - Firstly spaces are stripped from start and end of the string and as well as in between spaces are removed.
            - After which regex is applied to check wheather the entered value for email if valid or not.
      
      * **Check Numeric With No Spaces (Loan Account Number):**
            - In this spaces are stripped from start and end of the string and as well as in between spaces are removed.
            - The regex is applied which only allows alphanumeric values. If regex is fulfilled the valid is returned else invalid.
          
      * **Check Date:**
            - Firstly spaces are stripped from start and end of the string then it is check if the string is date or not if it's date then it is returned as valid else invalid.

      * **Check Name:**
            - Using regex values are verified and if it's valid then valid is returned else invalid.

      * **Check Product:**
            - Using regex values are verified and if it's valid then valid is returned else invalid.

      * **Check Number:**
            - The value entered here is converted to integer. If it's gets converted the it is returned as valid else invalid.

      * **Check Amount:**
            - The value entered here is converted to float. If it's gets converted the it is returned as valid else invalid.

      * **Check Alphanumeric:**
            - In this only data is clean, additional spaces present are removed.