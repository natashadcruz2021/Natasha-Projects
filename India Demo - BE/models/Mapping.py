"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Dict


@dataclass
class Mapping:
    _id: str
    product_type: str
    bucket_type: str
    job_id: str
    mapping: Dict[str, str]
    created_date: datetime
    modified_date: datetime = datetime.today().replace(microsecond=0)
    created_by: str = ''
    modified_by: str = ''
