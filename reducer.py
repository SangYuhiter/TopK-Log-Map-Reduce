# -*- coding: utf-8 -*-
"""
@File  : reducer.py
@Author: SangYu
@Date  : 2019/5/15 18:29
@Desc  : 第一次reduce
"""
from operator import itemgetter


def read_mapper_output(file):
    """
    以迭代器方式读取mapper的输出
    :param file:
    :return:
    """
    for line in open(file, "r", encoding="utf-8"):
        yield line.rstrip()


def topk_first_reducer():
    """
    第一次reduce:统计每个IP的访问次数及访问的时间列表
    """
    f_reducer = open("process_files/first_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/first_map.txt")
    log_dict = {}
    for line in data:
        line_split = line.split("\t")
        if line_split[0] in log_dict.keys():
            log_dict[line_split[0]].append(line_split[2])
        else:
            log_dict[line_split[0]] = [line_split[2]]
    for k in sorted(log_dict):
        access_time_list = log_dict[k]
        access_time_str = ""
        for item in access_time_list:
            access_time_str += item + " "
        f_reducer.write("%s\t%d\t%s\n" % (k, len(log_dict[k]), access_time_str.strip()))
    f_reducer.close()


def topk_second_reducer():
    """
    第二次reduce:使用访问次数进行排序，完成topk任务
    """
    f_reducer = open("process_files/second_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/second_map.txt")
    for item in sorted(data, reverse=True):
        f_reducer.write(item + "\n")
    f_reducer.close()


if __name__ == '__main__':
    # topk_first_reducer()
    topk_second_reducer()
