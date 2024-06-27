"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

import boto3
from configparser import ConfigParser
import logging

# logging.basicConfig(filename="aws.log", level=logging.INFO, format="%(asctime)s: %(levelname)s: %(message)s")

config = ConfigParser()
config.read('config/aws.ini')


class AWS:
    def __init__(self):
        self.aws_access_key_id = config.get('aws', 'AWS_SERVER_PUBLIC_KEY')
        self.aws_secret_access_key = config.get('aws', 'AWS_SERVER_SECRET_KEY')
        self.region_name = config.get('aws', 'REGION_NAME')

    def create_client(self):
        pass


class S3(AWS):
    def __init__(self):
        super().__init__()
        self.s3_client = boto3.client('s3', aws_access_key_id=self.aws_access_key_id, aws_secret_access_key=self.aws_secret_access_key, region_name=self.region_name)

    def create_client(self):
        return self.s3_client

    def retrieve_all_files_in_bucket(self, bucket_name: str, folder_name: str = ''):
        files = self.s3_client.list_objects_v2(Bucket=bucket_name, Prefix=folder_name)
        if files["ResponseMetadata"]["HTTPStatusCode"] == 200:
            return files["Contents"]
        else:
            logging.error(f'Could not fetch list of files in the {bucket_name} bucket')
            return []

    def upload_file(self, bucket_name: str, folder_path: str, filename: str) -> bool:
        try:
            with open(f'{folder_path}/{filename}', 'rb') as data:
                self.s3_client.upload_fileobj(Fileobj=data, Bucket=bucket_name, Key=filename)
        except Exception as e:
            print(e)
            logging.error(f'Could not upload the {filename} file into the {bucket_name} bucket -> {e}')
            return False
        return True

    def download_file(self, bucket_name: str, filename: str, download_file_path: str) -> bool:
        try:
            self.s3_client.download_file(Bucket=bucket_name, Key=filename, Filename=download_file_path)
        except Exception as e:
            logging.error(f'Could not download the {filename} file from the {bucket_name} bucket -> {e}')
            return False
        return True


s3 = S3()
print(s3.create_client())
# print(s3.retrieve_all_files_in_bucket('spocto-cms'))
print(s3.upload_file('spocto-cms', '/Users/dhruvaagrawal/Downloads', 'spocto_logo_transaparent.png'))
# print(s3.download_file('spocto-cms', 'file_example_XLS_10.xls', '/Users/dhruvaagrawal/Desktop/file_example_XLS_10.xls'))
