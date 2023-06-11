'''
Desc:
File: /main.py
File Created: Sunday, 11th June 2023 10:31:10 pm
Author: luxuemin2108@gmail.com
-----
Copyright (c) 2023 Camel Lu
'''
from core.save import save_etf, update_etf_indictor

if __name__ == '__main__':
    input_value = int(input("请输入下列序号执行操作:\n \
        1.入库ETF\n \
        2.更新ETF\n \
    输入："))
    if input_value == 1:
        save_etf()
    elif input_value == 2:
        update_etf_indictor()
