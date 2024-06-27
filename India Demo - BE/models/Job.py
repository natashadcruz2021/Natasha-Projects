"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime

from typing import List, Dict


@dataclass
class Job:
    _id: str
    name: str
    file_name: str
    file_size: str
    product_type: str
    bucket_type: str
    batch_no: int
    sales_order_no: int
    customer_id: int
    customer_name: str
    created_date: datetime
    modified_date: datetime = datetime.today().replace(microsecond=0)
    created_by: str = ''
    modified_by: str = ''
    row_count: int = None
    headers: List[str] = field(default_factory=list)
    header_mapping_datatype: Dict[str, str] = field(default_factory=dict)
    is_active: bool = True
    is_header_validated: bool = False
    is_data_in_engage: bool = False
    status_data_ingestion: str = 'Data Ingestion'
    status_segmentation: str = None
    status_campaign_plan: str = None
    campaign_start_date: datetime = None
    campaign_end_date: datetime = None
    campaign_dates: List[str] = None
