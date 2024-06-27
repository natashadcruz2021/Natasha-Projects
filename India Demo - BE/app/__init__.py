"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from flask import Flask
from flask_cors import CORS

from api import api
from api.v1 import v1
from api.v1.dataprep import dataprep
from api.v1.aws import aws
from api.v1.jobs import jobs
from api.v1.payments import payments
from api.v1.transfer import transfer
from api.v1.upload import upload
from config.config import FlaskConfig
from utils.functions.celery import init_celery

cors = CORS()


def create_app(config_class=FlaskConfig, **kwargs):
    app = Flask(__name__)
    cors.init_app(app)
    if kwargs.get('celery'):
        init_celery(kwargs.get('celery'), app)
    app.config.from_object(config_class)

    # Registering Blueprints
    v1.register_blueprint(upload)
    v1.register_blueprint(transfer)
    v1.register_blueprint(payments)
    v1.register_blueprint(jobs)
    v1.register_blueprint(dataprep)
    v1.register_blueprint(aws)
    api.register_blueprint(v1)
    app.register_blueprint(api)

    return app
