"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from datetime import datetime, timedelta

from flask import Blueprint, request

from constants import FUNCTION_MAPPING, DATATYPE_MAPPING
from schemas.API import create_response
from utils.classes.Job import Job
from utils.functions.jobs import process_metadata

jobs = Blueprint('jobs', __name__, url_prefix='/jobs')


@jobs.route('', methods=['GET'])
def fetch_all_jobs():
    order = request.args['order']
    skip = int(request.args['skip'])
    limit = int(request.args['limit'])
    job = Job()
    job_list = job.fetch_jobs(order=order, skip=skip, limit=limit)
    return create_response({
        'status': 200,
        'body': {**job_list},
        'headers': {'Content-Type': 'application/json'}
    })


@jobs.route('', methods=['POST'])
def create_job():
    data = request.get_json()
    job = Job()
    job_info = job.create_job(data)
    created_date = datetime.now()
    job_id = job_info['job_id']
    job_details = {'job_id': job_id, 'file_name': data['file_name'], 'created_date': created_date, 'exp_date': created_date + timedelta(1)}
    session_id, session_info, errors, header_mapping_datatype = process_metadata(job_details)
    job.update_job(id=job_id, update_params={'header_mapping_datatype': header_mapping_datatype, 'headers': list(header_mapping_datatype.keys())})
    if 'errors' in job_info:
        return create_response({
            'status': 400,
            'body': {**job_info},
            'headers': {'Content-Type': 'application/json'}
        })
    return create_response({
        'status': 201,
        'body': {
            **job_info,
            'header_mapping_datatype': header_mapping_datatype,
            'header_list': list(FUNCTION_MAPPING.keys()),
            'errors': errors,
        },
        'headers': {'Content-Type': 'application/json'}
    })


@jobs.route('/<string:id>', methods=['GET'])
def fetch_single_job(id):
    job = Job()
    job_detail = job.fetch_job_detail(id=id)
    return create_response({
        'status': 200,
        'body': {'data': [{**job_detail[0], 'mapping_datatypes': list(DATATYPE_MAPPING.values())}], 'total': len(job_detail)},
        'headers': {'Content-Type': 'application/json'}
    })


@jobs.route('/<string:id>', methods=['PATCH'])
def update_single_job(id):
    try:
        data = request.get_json()
        update_params = data['update_params']
        modified_date = datetime.today()
        job = Job()
        is_job_updated = job.update_job(id=id, update_params={**update_params, 'modified_date': modified_date})
        return create_response({
            'status': 200,
            'body': {'is_job_updated': is_job_updated}
        })
    except Exception as e:
        return create_response({
            'status': 500,
            'errors': {'message': 'Failed to deactivate job.', 'errors': {'args': e.args}}
        })


@jobs.route('/<string:id>', methods=['DELETE'])
def deactivate_job(id):
    modified_date = datetime.today()
    job = Job()
    is_job_updated = job.update_job(id=id, update_params={'is_active': False, 'modified_date': modified_date})
    if is_job_updated:
        return create_response({
            'status': 204
        })
    return create_response({
        'status': 500,
        'errors': {
            'message': 'Failed to deactivate job.'
        }
    })

