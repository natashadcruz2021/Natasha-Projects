"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations
from typing import Any, Dict, List
from datetime import datetime
import paramiko
from utils.functions import convert_bytes


class SFTP:
    def __init__(self, hostname: str, username: str, password: str):
        self.hostname = hostname
        self.username = username
        self.password = password

    def create_ssh_client(self):
        ssh_client = paramiko.SSHClient()
        try:
            ssh_client.connect(hostname=self.hostname, username=self.username, password=self.password)
        except paramiko.ssh_exception.SSHException as e:
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(hostname=self.hostname, username=self.username, password=self.password)
        return ssh_client

    def create_sftp_client(self):
        ssh_client = self.create_ssh_client()
        sftp_client = ssh_client.open_sftp()
        return sftp_client

    def view_files(self, remote_folder_path: str = None) -> List[Dict[str, Any]]:
        files = []
        sftp_client = self.create_sftp_client()
        print('SFTP Client', sftp_client, remote_folder_path)
        directory = sftp_client.listdir(remote_folder_path)
        print('Directory', directory)
        for file in directory:
            file_info = dict()
            file_info['name'] = file
            stats = sftp_client.lstat(f'{remote_folder_path}/{file}')
            file_info['stats'] = dict()
            file_info['stats']['size'] = convert_bytes(stats.st_size)
            file_info['stats']['modified_date'] = stats.st_mtime
            files.append(file_info)
        files.sort(key=lambda file: file['stats']['modified_date'])
        for file in files:
            file['stats']['modified_date'] = datetime.fromtimestamp(file['stats']['modified_date']).strftime('%d/%m/%Y, %H:%M:%S')
        return files

    def fetch_file(self, remote_file_path: str, local_file_path: str):
        sftp_client = self.create_sftp_client()
        files = sftp_client.get(remote_file_path, local_file_path)
        sftp_client.close()
        return files

    def create_file(self, remote_file_path: str, local_file_path: str):
        sftp_client = self.create_sftp_client()
        sftp_client.put(local_file_path, remote_file_path)
        sftp_client.close()
