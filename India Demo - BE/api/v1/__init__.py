"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Blueprint

v1 = Blueprint('v1', __name__, url_prefix='/v1')
