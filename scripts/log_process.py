# -*- coding: utf-8 -*-

from pyspark import SparkFiles
from pyspark.sql import SparkSession
import pandas as pd
import re


# 解析单个日志文件并提取所需的指标
def parse_log_file(spark, file_path):
    # 初始化一个字典来存储每个指标的值
    metrics = {
        'instructions': [],
        'icache-misses': [],
        'iTLB-misses': [],
        'branch-misses': [],
        'icache-misses(MPKI)': [],
        'iTLB-misses(MPKI)': [],
        'branch-misses(MPKI)': [],
    }

    # 定义正则表达式，匹配每个指标
    patterns = {
        'instructions': r'([\d,]+)\s+instructions',
        'icache-misses': r'([\d,]+)\s+L1-icache-load-misses',
        'iTLB-misses': r'([\d,]+)\s+iTLB-load-misses',
        'branch-misses': r'([\d,]+)\s+branch-misses',
    }
    
    content = spark.sparkContext.textFile(file_path).collect()
    for line in content:
        # 遍历每一行并查找每个指标的值
        for metric, pattern in patterns.items():
            match = re.search(pattern, line)  # 使用正则表达式匹配
            if match:
            # 捕获到的内容是字符串形式，可能包含逗号
                # 使用replace(',','')去除逗号，并将其转换为整数
                metrics[metric].append(int(match.group(1).replace(',', '')))
    
    ## 打开并读取日志文件
    #with open(file_path, 'r') as file:
    #    for line in file:
    #        # 遍历每一行并查找每个指标的值
    #        for metric, pattern in patterns.items():
    #            match = re.search(pattern, line)  # 使用正则表达式匹配
    #            if match:
    #                # 捕获到的内容是字符串形式，可能包含逗号
    #                # 使用replace(',','')去除逗号，并将其转换为整数
    #                metrics[metric].append(int(match.group(1).replace(',', '')))

    return metrics
# 计算MPKI
def parse_compute(parse):
    for i in range(len(parse["instructions"])):
        parse['icache-misses(MPKI)'].append(parse['icache-misses'][i]*1000/parse['instructions'][i])
        parse['iTLB-misses(MPKI)'].append(parse['iTLB-misses'][i]*1000/parse['instructions'][i])
        parse['branch-misses(MPKI)'].append(parse['branch-misses'][i]*1000/parse['instructions'][i])
        
    return parse
# 计算平均值
def parse_mean(parse):
    num = len(parse["instructions"])
    icache_mean = sum(parse['icache-misses(MPKI)']) / num
    iTLB_mean = sum(parse['iTLB-misses(MPKI)']) / num
    branch_mean = sum(parse['branch-misses(MPKI)']) / num
    return icache_mean, iTLB_mean, branch_mean


def process(spark, file_list, output_path=None):
    """
    :param file_list: 包含全部文件的绝对路径的list
    :param output_path: 本地输出csv的路径，None则不输出到磁盘
    :return: 返回处理好的dataframe
    """
    data = {
        "run_type": [],
        "mode": [],
        "icache_misses(MPKI)_mean": [],
        "iTLB_misses(MPKI)_mean": [],
        "branch_misses(MPKI)_mean": []
    }
    # file_path =  "F:/1/postgraduate/大规模课程作业/data/perfdata/"
    for file_path in file_list:
        file_name = file_path.split('/')[-1]
        run_type = file_name.split('.')[2]
        mode = file_name.split('.')[3]
        log_file_path = file_path
        parsed_metrics = parse_log_file(spark, log_file_path)
        parsed_metrics = parse_compute(parsed_metrics)
        icache_mean, iTLB_mean, branch_mean = parse_mean(parsed_metrics)

        data["run_type"].append(run_type)
        data["mode"].append(mode)
        data["icache_misses(MPKI)_mean"].append(icache_mean)
        data["iTLB_misses(MPKI)_mean"].append(iTLB_mean)
        data["branch_misses(MPKI)_mean"].append(branch_mean)
    # 将数据转换为Spark DataFrame
    rows = []
    for i in range(len(data["run_type"])):
        row = (
            data["run_type"][i],
            data["mode"][i],
            data["icache_misses(MPKI)_mean"][i],
            data["iTLB_misses(MPKI)_mean"][i],
            data["branch_misses(MPKI)_mean"][i],
        )
        rows.append(row)

    columns = ["run_type", "mode", "icache_misses(MPKI)_mean", "iTLB_misses(MPKI)_mean", "branch_misses(MPKI)_mean"]
    df = spark.createDataFrame(rows, schema=columns)
    df.write.mode("overwrite").option("header","true").csv(output_path)
    # df = pd.DataFrame(data)
    # df.to_csv(output_path, index=False)
    return df

if __name__ == '__main__':
    process(None, None)


