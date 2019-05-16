# -*- coding: utf-8 -*-
"""
@File  : GetTopK.py
@Author: SangYu
@Date  : 2019/5/16 9:27
@Desc  : 生成数据，并获取数据的topk
"""
from data_producer import *
from mapper import *
from reducer import *
import time


def get_topk(k: int):
    produce_data(100, "2019-05-01-00-00-00", 0.1)
    first_mapper()
    first_reducer()
    second_mapper()
    second_reducer()
    data = read_input("process_files/second_reduce.txt")
    print("前%d个数据"% k)
    while k > 0:
        print(data.__next__())
        k -= 1


if __name__ == '__main__':
    start = time.time()
    get_topk(5)
    print("耗时%s..." % (time.time() - start))
