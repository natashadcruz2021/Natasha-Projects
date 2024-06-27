"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Blueprint, request

from constants import ENGAGE_TABLE_COLUMNS, ENGAGE_COLUMNS_WITH_UNIQUE_VALUES
from schemas.API import create_response
from utils.classes.Job import Job
from utils.classes.Mapping import Mapping
from utils.functions import get_dict_keys_from_list
from utils.functions.transfer import move_file_to_engage

transfer = Blueprint('transfer', __name__, url_prefix='/transfer')


@transfer.route('/<string:id>/engage', methods=['GET', 'POST'])
def engage(id):
    if request.method == 'GET':
        mapping = Mapping()
        user_mapping, headers = mapping.fetch_mapping_details(job_id=id, param='mapping')
        new_user_mapping = get_dict_keys_from_list(mapping=user_mapping, headers=headers)
        return create_response({
            'status': 200,
            'body': {'engage_column_headers': list(ENGAGE_TABLE_COLUMNS.keys()), 'headers': headers, 'user_mapping': new_user_mapping}
        })
    else:
        data = request.get_json()
        is_file_moved = move_file_to_engage(job_id=id, options=data)
        if is_file_moved:
            return create_response({
                'status': 201,
                'body': {'message': 'File has been successfully moved to Engage.'}
            })
        return create_response({
            'status': 500,
            'body': {'message': f'File could not be moved to Engage.'}
        })


@transfer.route('/<string:id>/segmentation', methods=['GET'])
def get_segmentation_columns(id):
    """
    This method will only return mapping as key-value pairs with engage_columns that
    are most likely to not have all the values as distinct to create proper segments.
    """
    try:
        job = Job()
        mapping_details = job.fetch_job_detail(id=id, param='engage_mapping')
        formatted_mapping = {}
        for k, v in mapping_details.items():
            if v not in ENGAGE_COLUMNS_WITH_UNIQUE_VALUES:
                formatted_mapping[k] = v
        return create_response({
            'status': 200,
            'body': {'engage_mapping': formatted_mapping}
        })
    except Exception as e:
        return create_response({
            'status': 500,
            'body': {'message': 'Could not fetch columns for segmentation.', 'errors': {'args': e.args}}
        })
