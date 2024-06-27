import logging
from typing import List, Any

from constants import VALID_STATUS_COL


def create_chunks(array: List[Any], n: int) -> List[List[Any]]:
    chunks = []
    for i in range(0, len(array), n):
        chunks.append(array[i:i + n])
    return chunks


def remove_system_columns_from_headers(headers: List[str]) -> List[str]:
    try:
        headers.remove('_id')
        headers.remove(VALID_STATUS_COL)
    except ValueError as e:
        logging.error(f'Remove System Column Headers Function: -> Error: {e}')
    return headers
