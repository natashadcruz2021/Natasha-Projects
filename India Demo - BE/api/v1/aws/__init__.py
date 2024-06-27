"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Flask, Blueprint

aws = Blueprint('aws', __name__, url_prefix='/aws')


@aws.route('/', methods=['GET'])
def main():
    return 'Base URL for /aws.'
