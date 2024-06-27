"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from typing import Any, Dict, Tuple

from data_cleaning.data_validation import identify_header_datatype
from schemas.Redis import Redis
from utils.classes.Session import Session
from utils.functions import read_file


def process_metadata(job_details: Dict[str, Any]) -> Tuple[str, Dict[str, Any], Any, Dict[str, str]]:
    user_session = Session()
    session_id, session_info, errors = user_session.create_session(job_details)
    df = read_file(file_name=job_details['file_name'], no_of_rows=10)
    header_mapping_datatype = identify_header_datatype(df=df)
    return session_id, session_info, errors, header_mapping_datatype


def fetch_current_job_ref_id(redis: Redis, job_id: str) -> str:
    entry = redis.fetch_entries(id=job_id, key='jobs')
    current_job_ref_id = entry[job_id]
    return current_job_ref_id

