'''
Desc: 用接口获取股票信息
File: /base_api_config.py
File Created: Friday, 11th June 2021 1:58:37 pm
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2021 Camel Lu
'''

import json
import os
import logging
from dotenv import load_dotenv
from fake_useragent import UserAgent


class BaseApi:
    def __init__(self):
        logging.basicConfig(format='%(asctime)s %(levelname)s:%(message)s',
                            filename='log/api.log',  filemode='a', level=logging.INFO)
        load_dotenv()

    def set_client_headers(self, *,  cookie_env_key="xue_qiu_cookie", referer="https://xueqiu.com"):
        cookie = self.__dict__.get(cookie_env_key)
        cookie = cookie if cookie else os.getenv(cookie_env_key)
        ua = UserAgent()
        headers = {
            'Content-Type': 'application/x-www-form-urlencoded;charset=utf-8',
            # 'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.106 Safari/537.36',
            'User-Agent': ua.random.lstrip(),
            'Origin': referer,
            'Referer': referer,
            'Cookie': cookie
        }
        self.headers = headers
        return headers

    def get_data_from_json(self, path):
        with open(path) as json_file:
            return json.load(json_file)
