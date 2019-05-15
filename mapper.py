# -*- coding: utf-8 -*-
"""
@File  : mapper.py
@Author: SangYu
@Date  : 2019/5/15 18:27
@Desc  : 第一次map
"""
import os


def read_input(file):
    """
    读取文件
    :param file: 文件对象
    :return: 以迭代器方式每次返回文件对象的行
    """
    for line in open(file, "r", encoding="utf-8"):
        yield line.rstrip()


def mapper():
    # 读取日志文件
    f_map = open("first_map.txt", "w", encoding="utf-8")
    for file in os.listdir("data_files"):
        data = read_input(os.path.join("data_files", file))
        for line in data:
            line_split = line.split()
            ip = line_split[0]
            access_time = line_split[-1]
            f_map.write("%s\t%d\t%s\n" % (ip, 1, access_time))
    f_map.close()


if __name__ == '__main__':
    mapper()
