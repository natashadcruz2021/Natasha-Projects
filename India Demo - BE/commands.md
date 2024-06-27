## Start Celery Workers
celery -A celery_worker.celery worker --loglevel=INFO

## Run migration scripts
nohup python3 <file_name>.py