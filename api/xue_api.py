'''
Desc: 雪球API数据
File: /xue_api.py
File Created: Friday, 3rd September 2021 10:41:00 am
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2020 Camel Lu
'''
import os
import pandas as pd
import time
import logging
import dateutil
import requests
from bs4 import BeautifulSoup

from .base_api import BaseApi

from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

session = requests.Session()
retry = Retry(connect=3, backoff_factor=0.5)
adapter = HTTPAdapter(max_retries=retry)
session.mount('http://', adapter)
session.mount('https://', adapter)

indictor_key_map = {
    '市价': 'price',
    '成交量': 'volume',
    '成交额': 'amount',
    '振幅': 'amplitude',
    '单位净值': 'net_value',
    '累计净值': 'accum_net_value',
    '溢价率': 'premium_rate',
    '基金份额': 'share',
    '资产净值': 'asset',
    # '成立日': 'found_date'
}


class ApiXueqiu(BaseApi):
    def __init__(self):
        super().__init__()
        self.xue_qiu_cookie = os.getenv('xue_qiu_cookie')
        self.set_client_headers()

    def get_stock_page_info(self, symbol):
        """ 获取ETF到期时间等基本信息
        """
        url = "https://xueqiu.com/S/{}".format(symbol)
        # headers = self.get_client_headers()
        res = session.get(url, headers=self.headers)
        if res.status_code != 200:
            return None
        soup = BeautifulSoup(res.text, 'lxml')
        # print(soup.body.select('table.quote-info')[0].select('td'))
        etf_info = {}
        delist_flag = False
        for div in soup.body.select('.stock-flag'):
            if '退市' in div.text:
                delist_flag = True
                break
        if delist_flag:
            etf_info['delist_date'] = 1
        indictor_data = dict()
        for td in soup.body.select('table.quote-info')[0].select('td'):
            key = td.next[0:-1]
            if key in indictor_key_map.keys():
                value = td.span.text
                if key == '成交量':
                    if '万手' in value:
                        value = float(value.replace('万手', '')) * 10000
                    elif '亿手' in value:
                        value = float(value.replace('亿手', '')) * 100000000
                    elif '手' in value:
                        value = float(value.replace('手', ''))
                if isinstance(value, str):
                    if '万' in value:
                        value = float(value.replace('万', '')) * 10000
                    elif '亿' in value:
                        value = float(value.replace('亿', '')) * 100000000
                    elif '%' in value:
                        value = float(value.replace('%', ''))
                if isinstance(value, float):
                    value = round(value, 3)
                if value == '--':
                    value = None
                indictor_data[indictor_key_map[key]] = value
            elif '成立日' in td.text:
                etf_info['found_date'] = td.span.text
            elif '到期日：' in td.text and '--' not in td.span.text:
                print(symbol, td.getText(), td.span.text)
                etf_info['delist_date'] = td.span.text
            # 没有到期日去最新净值日期
            elif etf_info.get('delist_date') and '净值日期' in td.text:
                print(symbol, td.getText(), td.span.text)
                etf_info['delist_date'] = td.span.text
        etf_info['indictor'] = indictor_data
        return etf_info
