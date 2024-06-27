"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
from typing import Any, Dict
from dataclasses import dataclass
from datetime import datetime


@dataclass
class Recipe:
    _id: str
    action: str
    description: Dict[str, Any]
    created_date: datetime
    job_id: str
    col_count: int
    column_mapping: Dict[str, str] 
    rev_column_mapping: Dict[str, str]
    prev_node: str = None   # This field stores previous file_ref_id, a Doubly Linked List -> In case of Head, this will be 'null'
    next_node: str = None   # This field stores next file_ref_id, a Doubly Linked List -> In case of Tail, this will be 'null'
    modified_date: datetime = datetime.today().replace(microsecond=0)
    status: bool = True
    created_by: str = ''
    modified_by: str = ''
