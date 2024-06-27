"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import os
from flask import Flask, Blueprint, request
import json
from datetime import datetime, timedelta
import requests
from api.v1.upload.session import Session
from schemas.Redis import Redis
from schemas.API import create_response
from utils.classes.Cryptography import Cryptography

auth = Blueprint('auth', __name__, url_prefix='/auth')


@auth.route('/login', methods=['POST'])
def login():
    encrypted_credentials = request.get_json()
    crypto = Cryptography()
    decrypted_credentials = {key: json.loads(crypto.decrypt(value)) for key, value in encrypted_credentials.items()}
    auth_payload = {'userName': decrypted_credentials['username'], 'userPassword': decrypted_credentials['password']}
    response = requests.post('http://localhost:9090/authenticate', data=auth_payload)
    print(response.text)
    return create_response({
        'status': 200,
        'body': {'message': 'User successfully authenticated.'},
        'headers': {'Authorization': 'Bearer '}
    })


@auth.route('/sessions', methods=['GET', 'POST'])
def sessions():
    user_session = Session()
    if request.method == 'GET':
        session_info, errors = user_session.fetch_session()
        return create_response({
            'status': 200,
            'body': { 'sessions': session_info, 'errors': errors }
        })
    elif request.method == 'POST':
        data = request.get_json()
        session_id, session_info, errors = user_session.create_session(data)
        return create_response({
            'status': 201,
            'body': { 'sessionId': session_id, 'sessions': session_info, 'errors': errors }
        })


@auth.route('/sessions/<string:id>', methods=['GET', 'DELETE'])
def session(id):
    user_session = Session()
    if request.method == 'GET':
        session_info, errors = user_session.fetch_session(id)
        return create_response({
            'status': 200,
            'body': { 'sessions': session_info, 'errors': errors }
        })
    elif request.method == 'DELETE':
        deleted, errors = user_session.delete_session(id)
        return create_response({
            'status': 204,
            'body': { 'message': 'No Content', 'deleted': deleted, 'errors': errors }
        })

