'''
Desc: 上海证券交易所api
File: /sse_api.py
File Created: Monday, 6th December 2021 9:20:01 pm
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2021 Camel Lu
'''
import time
import json
import os
import requests

from .base_api import BaseApi


class ApiSSE(BaseApi):
    def __init__(self):
        super().__init__()
        self.sse_cookie = os.getenv('sse_cookie')
        self.set_client_headers(cookie_env_key="sse_cookie",
                                referer='http://www.sse.com.cn/')

    def get_etf_fund_list(self, *, subClass='01', fundType='00', beginPage=1):
        timestamp = int(time.time())
        callback = "jsonpCallback" + str(timestamp)
        url = "http://query.sse.com.cn/commonSoaQuery.do?jsonCallBack={jsonCallBack}&isPagination={isPagination}&sqlId={sqlId}&pageHelp.beginPage={beginPage}&fundType={fundType}&subClass={subClass}&_={timestamp}".format(
            jsonCallBack=callback,
            isPagination="false",
            sqlId="FUND_LIST",
            beginPage=beginPage,
            fundType=fundType,
            subClass=subClass,
            timestamp=timestamp
        )
        res = requests.get(url, headers=self.headers)
        try:
            if res.status_code == 200:
                data_text = res.text.replace(callback, '')[1:-1]
                res_json = json.loads(data_text)
                return res_json
            else:
                print('请求异常', res)
        except:
            raise ('中断')
