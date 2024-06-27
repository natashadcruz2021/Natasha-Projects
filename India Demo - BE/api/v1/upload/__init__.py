"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Blueprint, g

from constants import ALLOCATION_FOLDER, TASK_STATUS_FAIL, TASK_STATUS_SUCCESS, TASK_STATUS_CLIENT_ERROR
from schemas.API import create_response
from utils.classes.CustomErrors import InvalidInputError
from utils.functions.async_task_manager import async_api, status_polling
from utils.functions.file import save_file
from utils.functions.file import file_preview

upload = Blueprint('upload', __name__, url_prefix='/upload')

"""
The "/files" API is asynchronous in nature.
That has been achieved with the help of Threading.
URL for the post from which this has been picked up: https://stackoverflow.com/questions/31866796/making-an-asynchronous-task-in-flask
"""


@upload.route('/status/<string:task_id>', methods=['GET'])
def get_task_status(task_id):
    response = status_polling(task_id=task_id)
    return create_response(response)


@upload.route('/files', methods=['POST'])
@async_api(save_file)
def files():
    def task_runner(file_name):
        try:
            data, columns, pickle_file_name = file_preview(file_name=file_name, folder_type=ALLOCATION_FOLDER)
            return {
                'status': TASK_STATUS_SUCCESS,
                'file': {'data': data, 'total': len(data), 'column_headers': columns, 'file_name': pickle_file_name},
                'message': 'File has been successfully saved to the server.'
            }
        except InvalidInputError as e:
            return {
                'status': TASK_STATUS_CLIENT_ERROR,
                'error': e.args
            }
        except Exception as e:
            return {
                'status': TASK_STATUS_FAIL,
                'error': e.args
            }
    return task_runner
