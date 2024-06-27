"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from celery import Celery
from config.config import FlaskConfig


def make_celery(app_name=__name__):
    return Celery(
        app_name,
        backend=FlaskConfig.CELERY_BACKEND_URL,
        broker=FlaskConfig.CELERY_BROKER_URL
    )


celery = make_celery()
