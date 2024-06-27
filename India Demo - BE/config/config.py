"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from configparser import ConfigParser

customer_details_config = ConfigParser()
db_config = ConfigParser()
flask_config = ConfigParser()
mail_config = ConfigParser()
path_config = ConfigParser()

customer_details_config.read('config/customer_details.ini')
db_config.read('config/db.ini')
flask_config.read('config/flask.ini')
mail_config.read('config/mail.ini')
path_config.read('config/path.ini')


class FlaskConfig(object):
    SECRET_KEY = flask_config.get('flask', 'SECRET_KEY') or 'you-will-never-guess'
    SESSION_TYPE = 'filesystem'
    ALLOCATION_FOLDER = path_config.get('local', 'ALLOCATION_FOLDER')
    PAYMENT_FOLDER = path_config.get('local', 'PAYMENT_FOLDER')
    INVALID_FOLDER = path_config.get('local', 'INVALID_FOLDER')
    SQLALCHEMY_DATABASE_URI = db_config.get('mysql', 'HOST')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    LANGUAGES = ['en', 'es']
    CELERY_BACKEND_URL = 'redis://localhost:6379/1'
    CELERY_BROKER_URL = 'amqp://localhost/'
    CELERY_TASK_SERIALIZER = 'json'
    MONGODB_FETCH_LIMIT = 20
    MASKED_STRING_LENGTH = 10


class MS_DBConfig(object):
    HOST = db_config.get('mysql', 'HOST')
    PORT = db_config.get('mysql', 'PORT')
    USERNAME = db_config.get('mysql', 'USERNAME')
    PASSWORD = db_config.get('mysql', 'PASSWORD')
    DB_NAME = db_config.get('mysql', 'DB_NAME')


class ENGAGE_DBConfig(object):
    DRIVER = db_config.get('engage', 'DRIVER')
    HOST = db_config.get('engage', 'HOST')
    PORT = db_config.get('engage', 'PORT')
    USERNAME = db_config.get('engage', 'USERNAME')
    PASSWORD = db_config.get('engage', 'PASSWORD')
    DB_NAME = db_config.get('engage', 'DB_NAME')


class CIP_DBConfig(object):
    HOST = db_config.get('cip', 'HOST')
    PORT = db_config.get('cip', 'PORT')
    USERNAME = db_config.get('cip', 'USERNAME')
    PASSWORD = db_config.get('cip', 'PASSWORD')
    DB_NAME = db_config.get('cip', 'DB_NAME')
   

class PG_DBConfig(object):
    HOST = db_config.get('pg', 'HOST')
    PORT = db_config.get('pg', 'PORT')
    USERNAME = db_config.get('pg', 'USERNAME')
    PASSWORD = db_config.get('pg', 'PASSWORD')
    DB_NAME = db_config.get('pg', 'DB_NAME')


class ES_DBConfig(object):
    HOST = db_config.get('es', 'HOST')
    USERNAME = db_config.get('es', 'USERNAME')
    PASSWORD = db_config.get('es', 'PASSWORD')
    TRACE_INDEX = db_config.get('es', 'TRACE_INDEX')
    PAYMENT_INDEX = db_config.get('es', 'PAYMENT_INDEX')
    SUMMARY_INDEX = db_config.get('es', 'SUMMARY_INDEX')


class MailConfig(object):
    SMTP_PORT = mail_config.get('smtp-client', 'PORT')
    SMTP_SERVER_ADDRESS = mail_config.get('smtp-client', 'SERVER_ADDRESS')
    SMTP_SENDER_EMAIL = mail_config.get('smtp-client', 'SENDER_EMAIL')
    SMTP_RECEIVER_EMAIL = mail_config.get('smtp-client', 'RECEIVER_EMAIL')
    SMTP_PASSWORD = mail_config.get('smtp-client', 'PASSWORD')
    SMTP_CC_EMAIL = mail_config.get('smtp-client', 'CC_EMAIL')
    SENDGRID_API_KEY = mail_config.get('sendgrid', 'API_KEY')


class CustomerDetailsConfig(object):
    CUSTOMER_ID = customer_details_config.get('customer_info', 'CUSTOMER_ID')
    CUSTOMER_NAME = customer_details_config.get('customer_info', 'CUSTOMER_NAME')
    DEV_BASE_URL = customer_details_config.get('dev_server', 'BASE_URL')
    GENERATE_BATCH_URL = customer_details_config.get('dev_server', 'GENERATE_BATCH_URL')
