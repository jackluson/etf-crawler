'''
Desc: 工具函数
File: /index.py
Project: utils
File Created: Tuesday, 29th June 2021 11:27:46 am
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2021 Camel Lu
'''
import re
import time
import json
import logging
from threading import Thread, Lock
from functools import wraps

# %matplotlib inline


def timeit(func):
    @wraps(func)
    def timeit_wrapper(*args, **kwargs):
        start_time = time.perf_counter()
        result = func(*args, **kwargs)
        end_time = time.perf_counter()
        total_time = end_time - start_time
        print(
            f'Function {func.__name__}{args} {kwargs} Took {total_time:.4f} seconds')
        return result
    return timeit_wrapper


def get_symbol_by_code(stock_code):
    """
    根据code规则输出是上证还是深证
    """
    if bool(re.search("^(6|9)\d{5}$", stock_code)):
        symbol = 'SH' + stock_code
    elif bool(re.search("^(3|0|2)\d{5}$", stock_code)):
        symbol = 'SZ' + stock_code
    elif bool(re.search("^(4|8)\d{5}$", stock_code)):
        symbol = 'BJ' + stock_code
    else:
        print('code', stock_code, '未知')
    return symbol


def lock_process(func):
    lock = Lock()

    def wrapper(self, *args):
        lock.acquire()
        result = func(self, *args)
        lock.release()
        return result
    return wrapper
