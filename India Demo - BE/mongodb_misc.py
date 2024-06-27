"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from schemas.MongoDB import MongoDB

mongodb = MongoDB()

list_all_dbs = mongodb.fetch_databases()

print(list_all_dbs)
print(len(list_all_dbs))

for db in list_all_dbs:
    if db not in ['admin', 'config', 'local', 'universal']:
        mongodb.delete_database(db_name=db)

list_all_dbs = mongodb.fetch_databases()

print(list_all_dbs)
print(len(list_all_dbs))