from pyspark import SparkFiles
from pyspark.sql import SparkSession
import os

def list_hdfs_files(spark, directory):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark._jsc.hadoopConfiguration())
    path = spark._jvm.org.apache.hadoop.fs.Path(directory)
    
    if not fs.exists(path):
        print(f"路径 {directory} 不存在")
        return []
    
    # 获取目录下的所有文件
    files = fs.listStatus(path)
    file_list = [file.getPath().toString() for file in files if fs.isFile(file.getPath())]
    return file_list

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("ListHDFSFiles") \
        .getOrCreate()

    hdfs_directory = "hdfs://10.24.7.59:9000/data/perfdata"  # 修改为目标路径
    print(f"正在列出目录 {hdfs_directory} 下的文件...")
    # print(hdfs_directory)
    files = list_hdfs_files(spark, hdfs_directory)

    if files:
        print("文件列表：")
        for file in files:
            print(file)
    else:
        print("未找到文件或目录不存在。")
    
    spark.stop()
