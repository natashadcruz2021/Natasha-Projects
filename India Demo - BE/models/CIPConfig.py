"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from dataclasses import dataclass


@dataclass
class MobileCustomization:
    pass


@dataclass
class Checks:
    null: bool = True
    blanks: bool = True
    spaces: bool = True
    duplicates: bool = True
    invalid_characters: bool = True


@dataclass
class Config:
    customer_name: Checks
    mobile_no: Checks
    email_id: Checks
    account_no: Checks
    principal_outstanding: Checks
    total_outstanding: Checks
    emi_amount: Checks
    due_date: Checks
    # locations - geolocation, city, state, country
