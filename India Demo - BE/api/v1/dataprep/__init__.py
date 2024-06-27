"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Blueprint, request, current_app, send_file

from actions.Filters import Filters
from actions.Operations import Operations
from actions.RecipeActions import RecipeActions
from constants import MENU_OPTIONS_STATE_MAPPING, ORDER, INVALID_FOLDER, TASK_STATUS_SUCCESS, TASK_STATUS_FAIL, \
    TASK_STATUS_CLIENT_ERROR, \
    MENU_OPTIONS_SORT, MENU_OPTIONS_SEARCH, MENU_OPTIONS_MASK, MENU_OPTIONS_FILTER, MENU_OPTIONS_RANGE, \
    MENU_OPTIONS_RATIO, MENU_OPTIONS_RENAME, MENU_OPTIONS_CALCULATE_WAIVER, MENU_OPTIONS_ADD_DYNAMIC_MAPPING, \
    MENU_OPTIONS_ADD_STATIC_VALUE, MENU_OPTIONS_MERGE, MENU_OPTIONS_SPLIT, MENU_OPTIONS_DELETE_COLUMN, \
    SUMMARY_PROPERTY_MAX, SUMMARY_PROPERTY_UNIQUE, SUMMARY_PROPERTY_MIN, MENU_OPTIONS_ROUND_UP, MENU_OPTIONS_ROUND_DOWN, \
    MENU_OPTIONS_ROUND_OFF, MENU_OPTIONS_NUMBER_OF_YEARS
from schemas.API import create_response
from utils.classes.CustomErrors import InvalidInputError
from utils.classes.Summary import Summary
from utils.functions.analytics import process_file
from utils.functions.async_task_manager import status_polling, async_api
from utils.functions.file import create_file
from utils.functions.mail import create_and_send_mail

dataprep = Blueprint('dataprep', __name__, url_prefix='/dataprep')


@dataprep.route('/<string:id>/recipes', methods=['GET'])
def recipes(id):
    order = request.args.get('order')
    recipe = RecipeActions(job_id=id)
    result = recipe.fetch(order=order)
    return create_response({
        'status': 200,
        'body': result
    })


@dataprep.route('/<string:id>/undo', methods=['POST'])
def undo(id):
    dataset_actions = Filters(job_id=id)
    result = dataset_actions.undo()
    if 'errors' in result:
        return create_response({
            'status': 400,
            'body': result
        })
    return create_response({
        'status': 200,
        'body': result
    })


@dataprep.route('/<string:id>/redo', methods=['POST'])
def redo(id):
    action = Filters(job_id=id)
    result = action.redo()
    if 'errors' in result:
        return create_response({
            'status': 400,
            'body': result
        })
    return create_response({
        'status': 200,
        'body': result
    })


def validate_and_process_file_sync():
    """
    This function as of now can be categorized as not needed. But in case, in the future,
    validation needs to be handled to reject the API on the basis of the request received
    from the client, the validation can be added here, to not trigger the async function itself.
    """
    data = request.get_json()
    return data


@dataprep.route('/<string:id>/process', methods=['POST'])
@async_api(validate_and_process_file_sync)
def validate_and_process_file(id):
    def task_runner(data):
        try:
            result = process_file(job_id=id, header_mapping_datatype=data['header_mapping_datatype'])
            if 'errors' in result:
                return {
                    **result,
                    'status': TASK_STATUS_FAIL,
                }
            return {
                **result,
                'status': TASK_STATUS_SUCCESS,
            }
        except InvalidInputError as e:
            return {
                'status': TASK_STATUS_CLIENT_ERROR,
                'error': e.args
            }
        except Exception as e:
            return {
                'message': 'There was an error processing your file.',
                'errors': {'args': e.args},
                'status': TASK_STATUS_FAIL
            }

    return task_runner


@dataprep.route('/process/status/<string:task_id>', methods=['GET'])
def get_task_status(task_id):
    response = status_polling(task_id=task_id)
    return create_response(response)


@dataprep.route('/<string:id>/data', methods=['GET'])
def apply_filters(id):
    try:
        op = request.args.get('op')
        skip = int(request.args['skip'])
        limit = int(request.args['limit'])
        action = Filters(job_id=id)
        if op == MENU_OPTIONS_SEARCH:
            query = request.args.get('q')
            col = request.args.get('col')
            result = action.search(skip=skip, limit=limit, query=query, col=col)
        elif op == MENU_OPTIONS_SORT:
            order = int(ORDER[request.args.get('order')])
            col = request.args.get('col')
            result = action.sort(skip=skip, limit=limit, col=col, order=order)
        elif op == MENU_OPTIONS_FILTER:
            filter_args = dict(request.args)
            result = action.filter(skip=skip, limit=limit, filter_args=filter_args)
        else:
            result = action.all(skip=skip, limit=limit)
        return create_response({
            'status': 200,
            'body': result,
            'headers': {'Content-Type': 'application/json'}
        })
    except KeyError as e:
        return create_response({
            'status': 400,
            'body': {'errors': {'message': 'Invalid request made. Please specify the required query arguments.',
                                'args': [*e.args]}},
            'headers': {'Content-Type': 'application/json'}
        })


@dataprep.route('/<string:id>/data', methods=['POST'])
def apply_operations(id: str):
    options = request.get_json()
    try:
        results = None
        op = options['op']
        action = Operations(job_id=id)
        if op == MENU_OPTIONS_SPLIT:
            results = action.split(options=options)
        elif op == MENU_OPTIONS_MERGE:
            results = action.merge(options=options)
        elif op == MENU_OPTIONS_DELETE_COLUMN:
            results = action.delete_column(options=options)
        elif op == MENU_OPTIONS_RENAME:
            results = action.rename(options=options)
        elif op == MENU_OPTIONS_CALCULATE_WAIVER:
            results = action.calculate_value_post_waiver(options=options)
        elif op == MENU_OPTIONS_MASK:
            results = action.mask(options=options)
        elif op == MENU_OPTIONS_RANGE:
            results = action.range(options=options)
        elif op == MENU_OPTIONS_RATIO:
            results = action.ratio(options=options)
        elif op == MENU_OPTIONS_STATE_MAPPING:
            results = action.region_mapping(options=options)
        elif op in (MENU_OPTIONS_ROUND_UP, MENU_OPTIONS_ROUND_DOWN, MENU_OPTIONS_ROUND_OFF):
            results = action.round_numbers(options=options)
        elif op == MENU_OPTIONS_ADD_STATIC_VALUE:
            results = action.add_column_static_value(options=options)
        elif op == MENU_OPTIONS_ADD_DYNAMIC_MAPPING:
            results = action.add_column_dynamic_mapping(options=options)
        elif op == MENU_OPTIONS_NUMBER_OF_YEARS:
            results = action.calculate_number_of_years(options=options)
        return create_response({
            'status': 200,
            'body': results,
            'headers': {'Content-Type': 'application/json'}
        })
    except KeyError as e:
        return create_response({
            'status': 400,
            'body': {'errors': {'message': 'Invalid request made. Please specify the required query arguments.',
                                'args': [*e.args]}},
            'headers': {'Content-Type': 'application/json'}
        })


@dataprep.route('/<string:id>/data/columns', methods=['GET'])
def data_profile(id):
    query_params = request.args
    result = None
    try:
        if len(query_params) != 0:
            col_property = query_params.get('property')
            col_name = query_params.get('col_name')
            summary = Summary(job_id=id)
            if col_property == SUMMARY_PROPERTY_UNIQUE:
                result = summary.fetch_distinct_values_from_col(col_name=col_name)
            elif col_property in (SUMMARY_PROPERTY_MAX, SUMMARY_PROPERTY_MIN):
                result = summary.fetch_max_min_values_from_col(col_name=col_name, type=col_property)
        return create_response({
            'status': 200,
            'body': {'values': result}
        })
    except KeyError as e:
        return create_response({
            'status': 400,
            'body': {'errors': {'message': 'Invalid request made. Please specify the required query arguments.',
                                'args': [*e.args]}},
            'headers': {'Content-Type': 'application/json'}
        })


@dataprep.route('/<string:id>/invalid', methods=['GET'])
def fetch_invalid_records(id):
    try:
        skip = int(request.args['skip'])
        limit = int(request.args['limit'])
        action = Filters(job_id=id)
        result = action.invalid(skip=skip, limit=limit)
        return create_response({
            'status': 200,
            'body': result,
            'headers': {'Content-Type': 'application/json'}
        })
    except KeyError as e:
        return create_response({
            'status': 400,
            'body': {'errors': {'message': 'Invalid request made. Please specify the required query arguments.',
                                'args': [*e.args]}},
            'headers': {'Content-Type': 'application/json'}
        })


@dataprep.route('/<string:id>/invalid/files', methods=['GET', 'POST'])
def export_invalid_records(id):
    file_name = create_file(job_id=id, type='invalid')
    if file_name is not None:
        if request.method == 'GET':
            return send_file(f'{current_app.config[INVALID_FOLDER]}/{file_name}', as_attachment=True)
        else:
            # UI will send email address
            data = request.get_json()
            email_id = data.get('email_id')
            # Take this email address and send out email to this user
            is_mail_sent = create_and_send_mail(job_id=id, email_id=email_id, file_name=file_name)
            if is_mail_sent:
                return create_response({
                    'status': 200,
                    'body': {'message': 'Your mail was sent successfully.'},
                    'headers': {'Content-Type': 'application/json'}
                })
    return create_response({
        'status': 500,
        'body': {
            'errors': {
                'message': 'There was an error generating your file. Please contact the administrator for further assistance.'}
        },
        'headers': {'Content-Type': 'application/json'}
    })


@dataprep.route('/<string:id>/summary', methods=['GET'])
def get_data_summary(id):
    summary = Summary(job_id=id)
    result = summary.calculate()
    if 'errors' in result:
        return create_response({
            'status': 500,
            'body': {'message': 'Could not fetch summary for the current job.', **result}
        })
    return create_response({
        'status': 200,
        'body': result,
        'headers': {'Content-Type': 'application/json'}
    })
