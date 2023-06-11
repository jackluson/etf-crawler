'''
Desc: insert 相关语句
File: /insert.py
Project: sql_model
File Created: Thursday, 10th June 2021 11:20:43 am
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2021 Camel Lu
'''

from db.connect import connect
from utils.index import lock_process
from .base import BaseSqlModel
from lib.mysnowflake import IdWorker


class StockInsert(BaseSqlModel):
    def __init__(self):
        super().__init__()
        self.IdWorker = IdWorker()

    def generate_insert_sql(self, target_dict, table_name, ignore_list):
        # 拼接sql
        keys = 'id,' + ','.join(target_dict.keys())
        values = ','.join(['%s'] * (len(target_dict) + 1))
        update_values = ''
        for key in target_dict.keys():
            if key in ignore_list:
                continue
            update_values = update_values + '{0}=VALUES({0}),'.format(key)
        sql_insert = "INSERT INTO {table} ({keys}) VALUES ({values})  ON DUPLICATE KEY UPDATE {update_values}; ".format(
            table=table_name,
            keys=keys,
            values=values,
            update_values=update_values[0:-1]
        )
        return sql_insert

    def batch_insert_etf_fund(self, fund_list):
        etf_dict = {
            'code': '',
            'name': '',
            'full_name': '',
            'index_name': '',
            'index_code': '',
            'type': '',
            'company': '',
            # 'found_date': '',
            # 'delist_date': '',
            'market': ''
        }  # 保持与sq表l设计顺序一致
        etf_sql_insert = self.generate_insert_sql(
            etf_dict, 'etf_fund', ['id', 'code'])
        self.cursor.executemany(etf_sql_insert, fund_list)
        self.connect.commit()
