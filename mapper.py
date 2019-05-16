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


def first_mapper():
    # 读取日志文件
    f_map = open("process_files/first_map.txt", "w", encoding="utf-8")
    for file in os.listdir("data_files"):
        data = read_input(os.path.join("data_files", file))
        for line in data:
            line_split = line.split()
            ip = line_split[0]
            access_time = line_split[-1]
            f_map.write("%s\t%d\t%s\n" % (ip, 1, access_time))
    f_map.close()

def second_mapper():
    # 读取第一次reduce的结果
    f_map = open("process_files/second_map.txt","w",encoding="utf-8")
    data = read_input("process_files/first_reduce.txt")
    for line in data:
        line_split = line.split("\t")
        f_map.write("%s\t%s\t%s\n"%(line_split[1],line_split[0],line_split[2]))
    f_map.close()

if __name__ == '__main__':
    # first_mapper()
    second_mapper()
