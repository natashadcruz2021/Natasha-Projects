"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Blueprint, request, g, current_app

from constants import PAYMENT_FOLDER, TASK_STATUS_FAIL, TASK_STATUS_SUCCESS, PAYMENT_FILE_MAPPING
from schemas.API import create_response
from utils.classes.PaymentFile import PaymentFile
from utils.functions.async_task_manager import async_api, status_polling
from utils.functions.file import file_preview
from utils.functions.file import save_file

payments = Blueprint('payments', __name__, url_prefix='/payments')

"""
The "/files" API is asynchronous in nature.
That has been achieved with the help of Threading.
URL for the post from which this has been picked up: https://stackoverflow.com/questions/31866796/making-an-asynchronous-task-in-flask
"""


@payments.route('/status/<string:task_id>', methods=['GET'])
def get_task_status(task_id):
    response = status_polling(task_id=task_id)
    return create_response(response)


@payments.route('/files', methods=['POST'])
@async_api(save_file)
def files():
    def task_runner(file_name):
        try:
            data, columns, pickle_file_name = file_preview(file_name=file_name, folder_type=PAYMENT_FOLDER)
            return {
                'status' : TASK_STATUS_SUCCESS,
                'file': {'data': data, 'column_headers': columns, 'file_name': pickle_file_name},
                'message': 'File has been successfully saved to the server.'
            }
        except Exception as e:
            return {
                'status': TASK_STATUS_FAIL,
                'error': e.args
            }
    return task_runner


@payments.route('/<string:id>/mappings', methods=['GET'])
def get_user_mapping(id):
    """
    @param id:
    @query_args: file_name
    @return: headers, user_mapping
    """
    file_name = request.args.get('file_name')
    payment_file = PaymentFile(job_id=id)
    user_mapping, headers = payment_file.get_user_mapping(file_name=file_name)
    return create_response({
        'status': 200,
        'body': {'engage_column_headers': list(PAYMENT_FILE_MAPPING.keys()), 'headers': headers, 'user_mapping': user_mapping}
    })


def process_payment_file_sync_func():
    data = request.get_json()
    file_name = data['file_name']
    job_id = request.view_args['job_id']
    user_mappings = data['user_mapping']
    payment_file = PaymentFile(job_id=job_id)
    is_valid, err_msg = payment_file.validate(user_mappings,file_name=file_name)
    if is_valid is False:
        raise Exception(err_msg)
    return data


@payments.route('/<string:job_id>/process', methods=['POST'])
@async_api(process_payment_file_sync_func)
def process(job_id):
    def task_runner(data):
        try:
            file_name = data['file_name']
            user_mappings = data['user_mapping']
            payment_file = PaymentFile(job_id=job_id)
            is_file_moved = payment_file.process_payment_file(user_mappings, file_name=file_name)
            if is_file_moved:
                return {
                    'message': 'File has been successfully moved to Engage.',
                    'status': TASK_STATUS_SUCCESS,
                }
            return {
                'message': f'File could not be moved to Engage.',
                'status': TASK_STATUS_FAIL,
            }
        except Exception as e:
            return {
                'status': TASK_STATUS_FAIL,
                'error': str(e)
            }
    return task_runner


@payments.route('/process/status/<string:task_id>', methods=['GET'])
def get_process_status(task_id):
    response = status_polling(task_id=task_id)
    return create_response(response)

