"""
Author: Viral Mamniya
Author E-mail: viral.mamniya@spocto.com
"""

from __future__ import annotations
import re
from typing import Any, Callable, Dict, List
from dateutil.parser import parse

from constants import VALID_STATUS_COL
from utils.functions import isfloat


def check_email(email: str) -> Dict[str, Any]:
    """
    Function to check if email number is valid or not
    """
    try:
        # Remove whitespace from the beginning and end of a string.
        email = email.strip()
        # Remove spaces if present in between.
        email = email.replace(" ", "")
        regex = r"\b[A-Za-z0-9._+-]{1,}[A-Za-z0-9]+@[A-Za-z0-9]{1,}[A-Za-z0-9.-]+[A-Za-z0-9]{1,}\.[A-Z|a-z]{2,}\b"
        if re.fullmatch(regex, email):
            return {'value': email, VALID_STATUS_COL: True}
        else:
            return {'value': email, VALID_STATUS_COL: False}
    except Exception as e:
        return {'value': email, VALID_STATUS_COL: False}


def check_mobile(mobiles: str) -> List[Dict[str, Any]]:
    """
    Function to check number of mobile numbers in string with different delimiters.
    Split them and then check if there are valid (indian) mobile numbers.
    """
    try:
        # Split mobile string with delimiters.
        mobile_array = re.split(r'[;,|_\n]', mobiles)
        mobile_object_valid = []
        mobile_object_invalid = []
        for mobile in mobile_array:
            # Remove spaces around and trailing zero's to left and split by "." and consider 0th element.
            mobile = mobile.strip().lstrip('0').split('.')[0]
            # Remove all character except digits.
            mobile = re.sub('\D', '', mobile)
            if len(mobile) > 10:
                # If length is greater than 10 characters then check for country code if present then remove.
                first_two_char = mobile[:2]
                if first_two_char == '91':
                    mobile = mobile[2:]
                mobile = mobile.lstrip('0')
            temp_obj = {}
            # Check if number has all same characters then mark as invalid.
            if len(mobile) == 10:
                num_of_occurrence = mobile.count(mobile[0])
                if num_of_occurrence == len(mobile):
                    temp_obj['value'] = mobile
                    temp_obj[VALID_STATUS_COL] = False
                    mobile_object_valid.append(temp_obj)
                else:
                    temp_obj['value'] = mobile
                    temp_obj[VALID_STATUS_COL] = True
                    mobile_object_valid.append(temp_obj)
            else:
                temp_obj['value'] = mobile
                temp_obj[VALID_STATUS_COL] = False
                mobile_object_invalid.append(temp_obj)
        mobile_result = mobile_object_valid + mobile_object_invalid
        return mobile_result
    except ValueError as e:
        return [{'value': mobiles, VALID_STATUS_COL: False}]


def check_date(date: str, fuzzy: bool = False) -> Dict[str, Any]:
    """
    Check if date string contains date and is valid.
    If yes then convert to "YYYY-MM-DD HH:MM:SS" format.
    """
    try:
        # Remove whitespace from the beginning and end of a string.
        if isfloat(string=date) or date.isdigit():
            return {'value': date, VALID_STATUS_COL: False}
        date = date.strip()
        is_date = parse(date, fuzzy)
        is_date_format = str(is_date)[:20]
        if is_date:
            return {'value': is_date_format, VALID_STATUS_COL: True}
    except ValueError as e:
        return {'value': date, VALID_STATUS_COL: False}


def check_alphanumeric_no_space(value: str) -> Dict[str, Any]:
    """
    Check if string is alphanumeric and has no spaces (loan account number).
    """
    try:
        # Remove whitespace from the beginning and end of a string.
        value = value.strip().replace('  ', ' ')
        regex = r'^[a-zA-Z0-9]*$'
        if re.match(regex, value):
            return {'value': value, VALID_STATUS_COL: True}
        else:
            return {'value': value, VALID_STATUS_COL: False}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_alphanumeric(value: str) -> Dict[str, Any]:
    """
    Check if string is alphanumeric with space (name etc.).
    """
    try:
        # remove whitespace from the beginning and end of a string.
        value = value.strip().replace('  ', ' ')
        return {'value': value, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_amount(value: str) -> Dict[str, Any]:
    """
    Check if a valid amount is entered.
    """
    try:
        value = str(value).replace(',', '')
        value = float(value)
        return {'value': value, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_geo(value: str, nlp: Callable) -> Dict[str, Any]:
    """
    Check for 'location' datatype.
    """
    try:
        cleaned_value = value.lower().strip()
        cleaned_value = re.sub("[!@#$%^&*()[]{};:,./<>?\|`~-=_+]", " ", cleaned_value)
        cleaned_value = cleaned_value.replace("  ", " ")
        check_with_ner = nlp(cleaned_value)
        if check_with_ner is not None:
            final_output = [(ent.text, ent.label_) for ent in check_with_ner.ents]
            if final_output != [] and final_output is not None:
                if 'LOC' in final_output[0]:
                    return {'value': value, VALID_STATUS_COL: True}
                else:
                    return {'value': value, VALID_STATUS_COL: False}
            else:
                return {'value': value, VALID_STATUS_COL: False}
        else:
            return {'value': value, VALID_STATUS_COL: False}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_name(name: str) -> Dict[str, Any]:
    """
    Check if name is valid.
    """
    try:
        # remove whitespace from the beginning and end of a string.
        name = name.strip().replace("  ", " ")
        name = re.sub("[^A-Za-z.' ]", "", name)
        return {'value': name, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': name, VALID_STATUS_COL: False}


def check_product(product: str) -> Dict[str, Any]:
    """
    Check if the product name is valid.
    """
    try:
        # remove whitespace from the beginning and end of a string.
        product = product.strip().replace("  ", " ")
        regex = r'^[a-zA-Z-_/ ]*$'
        if re.match(regex, product):
            return {'value': product, VALID_STATUS_COL: True}
        else:
            return {'value': product, VALID_STATUS_COL: False}
    except Exception as e:
        return {'value': product, VALID_STATUS_COL: False}


def check_number(value: int) -> Dict[str, Any]:
    """
    Check if the number(integer) is valid.
    """
    try:
        value = str(value).replace(',', '')
        value = int(value)
        return {'value': value, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_float(value: float) -> Dict[str, Any]:
    """
    Check if the number(float) is valid.
    """
    try:
        value = str(value).replace(',', '')
        value = float(value)
        return {'value': value, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_text(value: str) -> Dict[str, Any]:
    try:
        # remove whitespace from the beginning and end of a string.
        value = value.strip().replace('  ', ' ')
        return {'value': value, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_bool(value: bool) -> Dict[str, Any]:
    try:
        # remove whitespace from the beginning and end of a string.
        value = value.strip().replace('  ', ' ')
        value_type = None
        if str(value).lower() in ("yes", "y", "true",  "t", "1"):
            value_type = True
        if str(value).lower() in ("no",  "n", "false", "f", "0", "0.0", ""):
            value_type = False
        if bool(value_type):
            return {'value': value, VALID_STATUS_COL: True}
        else:
            return {'value': value, VALID_STATUS_COL: False}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_derived_status(value: str) -> Dict[str, Any]:
    try:
        return {'value': value, VALID_STATUS_COL: True}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}


def check_geo_code(value: str) -> Dict[str, Any]:
    """
    Check if data contain valid country, city or state code.
    """
    try:
        # remove whitespace from the beginning and end of a string.
        value = value.strip().replace(' ', '')
        regex = r'^[0-9+]*$'
        if re.match(regex, value):
            return {'value': value, VALID_STATUS_COL: True}
        else:
            return {'value': value, VALID_STATUS_COL: False}
    except Exception as e:
        return {'value': value, VALID_STATUS_COL: False}
