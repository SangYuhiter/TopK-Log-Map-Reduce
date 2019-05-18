# -*- coding: utf-8 -*-
"""
@File  : reducer.py
@Author: SangYu
@Date  : 2019/5/15 18:29
@Desc  : 第一次reduce
"""
from operator import itemgetter
import time

time_format = "%Y-%m-%d-%H-%M-%S"


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
    f_reducer = open("process_files/topk_first_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/topk_first_map.txt")
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
    f_reducer = open("process_files/topk_second_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/topk_second_map.txt")
    for item in sorted(data, reverse=True):
        f_reducer.write(item + "\n")
    f_reducer.close()


def black_list_first_reducer():
    """
    使用时间作为键对日志记录进行排序
    """
    # 打开记录文件
    f_reducer = open("process_files/black_list_first_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/black_list_first_map.txt")
    for line in sorted(data):
        f_reducer.write("%s\n" % line)
    f_reducer.close()


def black_list_second_reducer(threshold:int):
    """
    归并每个IP的热点时间列表
    """
    f_reducer = open("process_files/black_list_second_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/black_list_second_map.txt")
    log_dict = {}
    for line in data:
        line_split = line.split("\t")
        ip = line_split[0]
        hot_time_split = line_split[1].split()
        ts = hot_time_split[0]
        time_struct = time.strptime(ts, time_format)
        ts_seconds = time.mktime(time_struct)
        te = hot_time_split[1]
        count = hot_time_split[2]
        if ip in log_dict.keys():
            # 查看热点时间列表的最后一个元素
            last_hot_time = log_dict[ip][-1]
            # 判断时间域是否小于阈值
            time_struct = time.strptime(last_hot_time["ts"], time_format)
            temp_seconds = time.mktime(time_struct)
            # 若小于则将计数值和te更新
            if ts_seconds - temp_seconds < threshold:
                log_dict[ip][-1]["te"] = te
                log_dict[ip][-1]["count"] = str(int(log_dict[ip][-1]["count"])+1)
            # 不满足则新建热点时间，添加
            else:
                log_dict[ip].append({"ts": ts, "te": te, "count": count})
        # 新建字典键
        else:
            log_dict[ip] = [{"ts": ts, "te": te, "count": count}]
    for k in sorted(log_dict):
        hot_time_list = log_dict[k]
        hot_time_str = ""
        for item in hot_time_list:
            for v in item.values():
                hot_time_str += v + " "
            hot_time_str = hot_time_str.strip()
            hot_time_str += ","
        f_reducer.write("%s\t%s\n" % (k, hot_time_str.strip()))
    f_reducer.close()


def black_list_third_reducer():
    f_reducer = open("process_files/black_list_third_reduce.txt", "w", encoding="utf-8")
    data = read_mapper_output("process_files/black_list_third_map.txt")
    for item in sorted(data, reverse=True):
        f_reducer.write(item + "\n")
    f_reducer.close()

if __name__ == '__main__':
    # topk_first_reducer()
    # topk_second_reducer()
    # black_list_first_reducer()
    # black_list_second_reducer()
    black_list_third_reducer()
