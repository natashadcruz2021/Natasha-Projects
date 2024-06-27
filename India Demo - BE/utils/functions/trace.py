
from schemas.MongoDB import MongoDB
from utils.functions.transfer import process_engage_data
from utils.classes.Job import Job
import logging


def move_data_to_trace():
    mongodb = MongoDB()
    data = mongodb.fetch_entries(collection_name='jobs', filters={'moved_to_trace': { '$exists': False }, 'user_mapping':  { '$exists': True }})
    for job_data in data['data']:
        try:
            job_id = job_data['_id']
            process_engage_data(job_id=job_id, table_name='spocto_trace')
            job = Job()
            job.update_job(id=job_id, update_params={'moved_to_trace': True})
        except Exception as e:
            pass
            # logging.error(f'''ERROR in moving job_id: {job_data['_id']} to trace  -> {e} ''')
