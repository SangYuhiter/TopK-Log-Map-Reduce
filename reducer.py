# -*- coding: utf-8 -*-
"""
@File  : reducer.py
@Author: SangYu
@Date  : 2019/5/15 18:29
@Desc  : 第一次reduce
"""
from operator import itemgetter
from itertools import groupby

def read_mapper_output(file):
    """
    以迭代器方式读取mapper的输出
    :param file:
    :return:
    """
    for line in open(file, "r", encoding="utf-8"):
        yield line.rstrip().split("\t")

def reducer():
    f_reducer = open("first_reduce.txt","w",encoding="utf-8")
    data = read_mapper_output("first_map.txt")
    for current_ip, group in groupby(data,itemgetter(0)):
        print(current_ip,group.__sizeof__())
        # print(group)
        # try:
        #     total_count = sum(int(count) for current_ip,count,access_time in group)
        #     access_time_list = [access_time for current_ip,count,access_time in group]
        #     access_time_str = ""
        #     for access_time in access_time_list:
        #         access_time_str +=access_time+","
        #     f_reducer.write("%s\t%d\t%s\n"%(current_ip,total_count,access_time_str))
        # except ValueError:
        #     pass
    f_reducer.close()

if __name__ == '__main__':
    reducer()


