"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Blueprint

from schemas.API import create_response
from schemas.MongoDB import MongoDB
from schemas.MySQL import MySQL
from schemas.Redis import Redis

api = Blueprint('api', __name__, url_prefix='/api')


@api.route('/health', methods=['GET'])
def health():
    mongodb = MongoDB()
    # mysql = MySQL()
    redis = Redis()
    return create_response({
        'status': 200,
        'body': {
            'message': 'Health check API.',
            'mongodb': mongodb.ping(),
            # 'mysql': mysql.ping(),
            'redis': redis.ping()
        }
    })


@api.route('/metrics', methods=['GET'])
def metrics():
    return create_response({
        'status': 200,
        'body': { 'message': 'Metrics API.' }
    })


@api.route('/debug', methods=['GET'])
def debug():
    mongodb = MongoDB()
    return create_response({
        'status': 200,
        'body': { 'message': 'Debug API.', 'mongodb': mongodb.get_server_info() }
    })


@api.route('/cleanup', methods=['DELETE'])
def cleanup():
    """
    This API cleans up all the DBs.
    But before that, on export, all the existing dbs have all but one collection removed. When
    this API is called, it collates all the tables and puts them into one db = 'universal'.
    It also empties the job list. This process takes into consideration that all files that are
    older than 15 days ONLY will get deleted.
    """
    pass
