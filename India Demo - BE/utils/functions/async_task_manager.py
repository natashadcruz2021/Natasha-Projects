"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import logging
import threading
import uuid
from functools import wraps

from flask import request, current_app
from werkzeug.exceptions import HTTPException, InternalServerError
from constants import TASK_STATUS_FAIL, TASK_STATUS_SUCCESS, TASK_STATUS_CREATED, TASK_STATUS_CLIENT_ERROR

from schemas.API import create_response
from schemas.Redis import Redis


def async_api(sync_func):
    def async_api_internal(wrapped_function):
        @wraps(wrapped_function)
        def new_function(*args, **kwargs):
            redis = Redis(db=0)

            def task_call(flask_app, environ, extra_param):
                # Create a request context similar to that of the original request
                # so that the task can have access to flask.g, flask.request, etc.
                with flask_app.request_context(environ):
                    task = redis.fetch_set_entries(name=task_id)
                    try:
                        task['return_value'] = wrapped_function(*args, **kwargs)(extra_param)
                    except HTTPException as e:
                        task['return_value'] = current_app.handle_http_exception(e)
                    except Exception as e:
                        print(e)
                        # The function raised an exception, so we set a 500 error
                        logging.error(f'Async Upload API Task Call Function: Task ID: {task_id} --> Error: {e}')
                        task['return_value'] = InternalServerError()
                        if current_app.debug:
                            # We want to find out if something happened so reraise
                            raise
                    redis.create_expiring_set_entries(name=task_id, value=task)

            """
            - Assign an ID to the asynchronous task (i.e. Task ID)
            - Save File to the server
            - Setting an initial value to allow for 404 error
            - Record the task, and then launch it
            - Return a 202 response, with a Task ID that the client can use to obtain task status
            """
            try:
                task_id = uuid.uuid4().hex
                extra_param = sync_func()
                redis.create_expiring_set_entries(name=task_id, value={'init': 'value'})
                task = {'task_thread': threading.Thread(target=task_call, args=(current_app._get_current_object(), request.environ, extra_param))}
                task['task_thread'].start()
                return create_response({
                    'status': 202,
                    'body': {
                        'message': 'Your request has been received. It is being processed.',
                        'task_id': task_id
                    }
                })
            except Exception as e:
                return create_response({
                    'status': 500,
                    'body': {
                        'message': 'Your request could not be processed.',
                        'error': e.args
                    }
                })
        return new_function
    return async_api_internal


def status_polling(task_id: str):
    """
    Return status about an asynchronous task. If this request returns a 202
    status code, it means that task hasn't finished yet. Else, the response
    from the task is returned.
    """
    redis = Redis(db=0)
    is_key_exists = redis.exists(name=task_id)
    if not is_key_exists:
        return {
            'status': 404,
            'body': {'message': 'Your file could not be found. Please try again.'}
        }
    task = redis.fetch_set_entries(name=task_id)
    if isinstance(task, dict) and task['init'] == 'value' and len(task) == 1:
        return {
            'status': 202,
            'body': {
                'message': 'File is still being processed.',
                'task_id': task_id
            }
        }
    if isinstance(task['return_value'], dict) and 'status' in task['return_value']:
        if task['return_value']['status'] == TASK_STATUS_SUCCESS:
            return {
                'status': 200,
                'body': {**task['return_value']}
            }
        elif task['return_value']['status'] == TASK_STATUS_CREATED:
            return {
                'status': 201,
                'body': {**task['return_value']}
            }
        elif task['return_value']['status'] == TASK_STATUS_CLIENT_ERROR:
            return {
                'status': 400,
                'body': {**task['return_value']}
            }
        elif task['return_value']['status'] == TASK_STATUS_FAIL:
            return {
                'status': 500,
                'body': {**task['return_value']}
            }
    # Handles all errors - InternalServerError() and HTTPException
    return {
        'status': 500,
        'body': {'message': 'The server encountered an error. Please try again.'}
    }
