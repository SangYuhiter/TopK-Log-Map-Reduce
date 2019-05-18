# -*- coding: utf-8 -*-
"""
@File  : BlackList.py
@Author: SangYu
@Date  : 2019/5/18 11:39
@Desc  : 获取黑名单
"""
from data_producer import *
from mapper import *
from reducer import *
import time


def get_black_list(threshold: int, number: int):
    """
    获得黑名单，在时间阈值threshold内访问了number次
    :param threshold: 时间阈值
    :param number: 访问次数
    """
    produce_data(100, "2019-05-01-00-00-00", 0.01)
    start = time.time()
    black_list_first_mapper()
    print("first_map end!")
    black_list_first_reducer()
    print("first_reduce end!")
    black_list_second_mapper()
    print("second_map end!")
    black_list_second_reducer(threshold)
    print("second_reduce end!")
    black_list_third_mapper()
    print("third_map end!")
    black_list_third_reducer()
    print("third_reduce end!")
    print("耗时%s..." % (time.time() - start))
    data = read_input("process_files/black_list_third_reduce.txt")
    print("黑名单数据")
    for line in data:
        line_split = line.split("\t")
        count = int(line_split[0])
        if count >= number:
            print(line)
        else:
            break


if __name__ == '__main__':
    get_black_list(10, 5)
