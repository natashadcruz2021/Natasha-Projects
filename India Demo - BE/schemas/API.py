"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
from flask import make_response, jsonify, Response
from typing import Any, Dict


def create_response(response_object: Dict[str, Any]) -> "Response":
    response = make_response(
        jsonify(response_object['body']),
        response_object['status']
    )
    if 'headers' in response_object.keys():
        response = make_response(
            jsonify(response_object['body']),
            response_object['status'],
            response_object['headers']
        )
    return response
