"""
Author: Dhruva Agrawal
Author E-mail: dhruva.agrawal@spocto.com
"""

from __future__ import annotations

from typing import Any, Dict, Tuple
from uuid import uuid1

import numpy as np
from pandas import DataFrame

from actions.RecipeActions import RecipeActions
from schemas.MongoDB import MongoDB
from schemas.Redis import Redis
from utils.functions.analytics import mask_values
from utils.functions.operations import compute_undo_redo


class Operation:
    def __init__(self, job_id: str, operation: str):
        self.job_id = job_id
        self.operation = operation
        self.redis = Redis(db=1)
        self.mongodb = MongoDB()

    def apply(self, options: Dict[str, str], df: 'DataFrame') -> Tuple:
        skip = options['skip']
        limit = options['limit']
        username = options['username']
        description = ''

        if self.operation == 'Split':
            new_col1 = options['new_col1']
            new_col2 = options['new_col2']
            col = options['col']
            separator = options['separator']
            description = f'Column "{col}" was split into "{new_col1}" and "{new_col2}" using the separator "{separator}".'
            df[[new_col1, new_col2]] = df[col].str.split(separator, n=1, expand=True)

        elif self.operation == 'Merge':
            new_col = options['new_col']
            separator = options['separator']
            col1 = options['col1']
            col2 = options['col2']
            description = f'Columns "{col1}" and "{col2}" were merged into "{new_col}" using the separator "{separator}".'
            df = df.astype({col1: str, col2: str})
            df[new_col] = df[col1] + separator + df[col2]

        elif self.operation == 'Delete Column':
            col = options['col']
            description = f'Column "{col}" was deleted.'
            df.drop(columns=[col], inplace=True)

        elif self.operation == 'Rename':
            prev_col = options['prev_col']
            new_col = options['new_col']
            description = f'Column "{prev_col}" was renamed to "{new_col}".'
            df.rename(columns={prev_col: new_col}, inplace=True)

        elif self.operation == 'Waiver Value Calculation':
            col_name = options['col_name']
            percentage = options['percentage']
            description = f'Column "{col_name}_post_waiver" was created post {percentage}% waiver calculation on "{col_name}".'
            df[f'{col_name}_post_waiver'] = df[col_name] * (1 - (int(percentage) / 100))

        elif self.operation == 'Mask':
            columns = options['columns']
            no_of_unmasked_char = options['no_of_unmasked_char']
            column_list = ','.join(columns)
            description = f'Columns "{column_list}" were masked with {no_of_unmasked_char} characters left visible.'
            for column in columns:
                df[column] = df[column].apply(mask_values, count=no_of_unmasked_char)

        elif self.operation == 'Range':
            lower_range = options['lower_range']
            higher_range = options['higher_range']
            col_name = options['col_name']
            description = f'Column "{col_name}_range" was created for {lower_range} and {higher_range} applied on "{col_name}".'
            df[f'{col_name}_range'] = np.where((df[col_name] <= higher_range) & (df[col_name] >= lower_range),
                                               f'{lower_range}-{higher_range}', df[col_name])

        elif self.operation == 'Ratio':
            col1 = options['col1']
            col2 = options['col2']
            description = f'Column "{col1}_{col2}_ratioed" was created for ratio of "{col1}" and "{col2}".'
            df[f'{col1}_{col2}_ratioed'] = df[col1] / df[col2]
            df.replace([np.inf, -np.inf], np.nan, inplace=True)

        action_details = {'action': self.operation, 'username': username, 'description': description}
        return df, action_details, skip, limit

    def process(self, df: DataFrame, skip: int, limit: int, current_job_ref_id: str, action_details: Dict[str, Any]):
        new_job_ref_id = str(uuid1())
        df.replace({np.nan: None, 'nan': None}, inplace=True)
        data = df.to_dict(orient='records')
        is_data_created = self.mongodb.create_entries(db_name=self.job_id, collection_name=new_job_ref_id, data=data)
        recipe = RecipeActions(job_id=self.job_id)
        is_recipe_created = recipe.add(action_details=action_details, current_job_ref_id=current_job_ref_id, new_job_ref_id=new_job_ref_id)
        if is_data_created and is_recipe_created:
            self.redis.create_entries(id=self.job_id, key='jobs', data=new_job_ref_id)
            results = self.mongodb.fetch_entries(db_name=self.job_id, collection_name=new_job_ref_id, skip=skip, limit=limit, get_total=True)
            is_undo_possible, is_redo_possible = compute_undo_redo(job_id=self.job_id, mongodb=self.mongodb, redis=self.redis)
            return {**results, 'is_undo_possible': is_undo_possible, 'is_redo_possible': is_redo_possible}
        return {'data': [], 'errors': 'Could not create new file reference id.'}
