"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import atexit
import logging

from apscheduler.schedulers.background import BackgroundScheduler

from app import create_app
from app.celery import celery
from constants import SYSTEM_DATETIME_FORMAT
from schemas.API import create_response
from utils.functions.trace import move_data_to_trace

app = create_app(celery=celery)

# logging.basicConfig(filename='logs/sys.log', encoding='utf-8', level=logging.DEBUG, format='[%(asctime)s] {%(pathname)s:%(lineno)d} %(levelname)s - %(message)s', datefmt=SYSTEM_DATETIME_FORMAT)

"""
Use the 'before_request' method with 'app' as '@app.before_request'
to setup authentication of user as per the information in the user session
record stored in Redis.
"""


@app.route('/', methods=['GET'])
def main():
    return create_response({
        'status': 200,
        'body': {'message': 'Service is up and running.'}
    })


@app.route('/site-map', methods=['GET'])
def site_map():
    return app.url_map


#scheduler = BackgroundScheduler()
#scheduler.add_job(func=move_data_to_trace, trigger="interval", seconds=60)
#scheduler.start()
# Shut down the scheduler when exiting the app
#atexit.register(lambda: scheduler.shutdown())

if __name__ == '__main__':
    app.run(debug=True, port=5000)
