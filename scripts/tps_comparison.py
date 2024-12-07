import os
import io
import pandas as pd
import matplotlib.pyplot as plt
from pyspark import SparkFiles
from pyspark.sql import SparkSession

file_name = "tps_comparison.png"

def process(spark, csv_path, img_path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path
    img_file = f"{img_path}{file_name}"
    print(img_file)
    data = spark.read.csv(csv_path, header=True, inferSchema=True)
    data = data.toPandas()
    #提取事件列（即指标列）
    category_column = data.columns[1]
    x_label_column = data.columns[0]
    events = data.columns[2:]
    # 去掉 'delete' 的行
    data = data[data[category_column] != 'delete']
    # 为每个事件生成柱状图
    for event in events:
        plt.figure(figsize=(10, 6))
        x_positions = range(len(data[x_label_column].unique()))
        bar_width = 0.3
        modes = data[category_column].unique()

        # 动态设置Y轴范围
        y_max = float(data[event].astype(float).max())
        y_ticks = range(0, int(y_max) + 2, 10000)
        y_labels = [str(y) for y in y_ticks]

        for i, mode in enumerate(modes):
            subset = data[data[category_column] == mode]
            plt.bar(
                [pos + i * bar_width for pos in x_positions],
                subset[event].astype(float),
                bar_width,
                label=mode,
            )

        # 设置x轴刻度
        plt.xticks(
            [pos + (len(modes) - 1) * bar_width / 2 for pos in x_positions],
            data[x_label_column].unique(),
        )

        # 设置图表标题和标签
        plt.title("TPS Comparison", fontsize=14)
        plt.xlabel("Type", fontsize=12)
        plt.ylabel(event, fontsize=12)
        plt.yticks(y_ticks, y_labels)

        # 显示柱状图顶部的值
        for i, mode in enumerate(modes):
            subset = data[data[category_column] == mode]
            for j, value in enumerate(subset[event].astype(float)):
                x = j + i * bar_width
                plt.text(
                    x, value + 0.1, f"{value:.2f}", ha="center", va="bottom", fontsize=8
                )

        plt.legend(title="Mode")

        # 保存图像
        buffer = io.BytesIO()
        plt.savefig(buffer, format='png')
        plt.close()
        buffer.seek(0)
        data_bytes = buffer.read()
        img_file = Path(img_file)
        output_stream = fs.create(img_file, True)
        output_stream.write(bytearray(data_bytes))
        output_stream.close()

        #  output_file = os.path.join(output_dir, "TPS.micro_tps.png")
        #  plt.savefig(output_file)
        plt.close()

    print(f"柱状图已生成并保存在 {img_file} 文件夹中。")
