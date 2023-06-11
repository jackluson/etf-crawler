'''
Desc: query 相关语句
File: /query.py
Project: sql_model
File Created: Friday, 11th June 2021 12:59:09 pm
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2021 Camel Lu
'''
from datetime import datetime
from .base import BaseSqlModel


class StockQuery(BaseSqlModel):
    def __init__(self):
        super().__init__()

    def query_etf(self, found_date=None):
        """
        查询ETF
        Args:
            market ([str]): ['sh', 'sz']
        """
        found_date = found_date if found_date else datetime.now().strftime("%Y-%m-%d")
        query_stock_sql = "SELECT a.code, a.name, a.market FROM etf_fund as a WHERE a.delist_date IS NULL AND a.found_date <= %s"
        self.dict_cursor.execute(query_stock_sql, [found_date])

        results = self.dict_cursor.fetchall()
        return results
