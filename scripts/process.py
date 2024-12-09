from pyspark import SparkFiles
from pyspark.sql import SparkSession
import os
from custom_modules import log_process,txt_process,tps_comparison,event_comparison

server_host = "hdfs://hadoop01:9000"
log_directory = f"{server_host}/data/perfdata/"
log_output_path = f"{server_host}/data/csvs/mysqld_micro.csv"
txt_directory = f"{server_host}/data/script_output/"
txt_output_path = f"{server_host}/data/csvs/mysqld_tps.csv"
img_path = f"{server_host}/data/imgs/"


def list_hdfs_files(spark, directory):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(directory)
    
    if not fs.exists(path):
        print(f"路径 {directory} 不存在")
        return []
    
    files = fs.listStatus(path)
    file_list = [file.getPath().toString() for file in files if fs.isFile(file.getPath())]
    return file_list

def solve_log_files(spark, directory, output_path):
    file_list = list_hdfs_files(spark, directory)
    df = log_process.process(spark, file_list, output_path)


def solve_txt_files(spark, directory, output_path, img_path):
    file_list = list_hdfs_files(spark, directory)
    txt_process.process(spark, file_list, output_path, img_path)

def draw_imgs(spark):
    tps_comparison.process(spark, txt_output_path, img_path)
    event_comparison.process(spark, log_output_path,img_path)

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("spark_experiment") \
        .getOrCreate()

    solve_log_files(spark, log_directory, log_output_path)
    solve_txt_files(spark, txt_directory, txt_output_path,img_path)
    draw_imgs(spark)

    spark.stop()
