"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from app import create_app
from app_celery import celery
from utils.functions.celery import init_celery

app = create_app()
init_celery(celery, app)
