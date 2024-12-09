import os
import io
from pyspark.sql import functions as F
import matplotlib.pyplot as plt
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def process(spark, csv_path, img_path):
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())
    Path = spark._jvm.org.apache.hadoop.fs.Path
    data = spark.read.csv(csv_path, header=True, inferSchema=True)
    # 提取事件列（即指标列）
    category_column = data.columns[0]  # 假设第一列是分类列（比如 mode）
    x_label_column = data.columns[1]  # 假设第二列是 x 轴的分类列
    events = data.columns[2:]
    data = data.filter(data[x_label_column].isNotNull())
    modes = data.select(category_column).distinct().rdd.flatMap(lambda x: x).collect()
    x_labels = data.select(x_label_column).distinct().rdd.flatMap(lambda x: x).collect()
    x_positions = range(len(x_labels))

    # 为每个事件生成柱状图
    for event in events:
        plt.figure(figsize=(10, 6))
        bar_width = 0.2

        # 动态设置Y轴范围
        y_max = data.select(F.max(F.col(event).cast("float"))).first()[0]
        y_ticks = range(0, int(y_max) + 2)
        y_labels = [str(y) for y in y_ticks]

        for i, mode in enumerate(modes):
            subset = data.filter(data[category_column] == mode).select(x_label_column, event)
            mode_data = subset.groupBy(x_label_column).agg(F.sum(event).cast("float").alias(event)).orderBy(x_label_column)
            mode_values = mode_data.rdd.map(lambda row: row[1]).collect()

            plt.bar(
                [pos + i * bar_width for pos in x_positions],
                mode_values,
                bar_width,
                label=mode,
            )

        # 设置x轴刻度
        plt.xticks(
            [pos + (len(modes) - 1) * bar_width / 2 for pos in x_positions],
            x_labels,
        )

        # 设置图表标题和标签
        plt.title(f"{event} Comparison", fontsize=14)
        plt.xlabel("Type", fontsize=12)
        plt.ylabel(event, fontsize=12)
        plt.yticks(y_ticks, y_labels)

        # 显示柱状图顶部的值
        for i, mode in enumerate(modes):
            subset = data.filter(data[category_column] == mode).select(x_label_column, event)
            mode_data = subset.groupBy(x_label_column).agg(F.sum(event).cast("float").alias(event)).orderBy(x_label_column)
            mode_values = mode_data.rdd.map(lambda row: row[1]).collect()
            for j, value in enumerate(mode_values):
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
        img_file = f"{img_path}{event}_comparison.png"
        img_file = Path(img_file)
        output_stream = fs.create(img_file, True)
        output_stream.write(bytearray(data_bytes))
        output_stream.close()
        plt.close()

    print(f"柱状图已生成并保存在 {img_file} 文件夹中。")
