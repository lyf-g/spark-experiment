import os
import re
import csv
import io
import matplotlib.pyplot as plt
from pyspark import SparkFiles
from pyspark.sql import SparkSession


def plot_tps_trend(spark, file_path, output_dir, bolt_mode, run_type):
    """ Plot the TPS trends for given runs within a file. """
    tps_re = re.compile(r"transactions:\s+\d+\s+\((\d+\.\d+) per sec\.\)")
    if bolt_mode is None or run_type is None:
        match = re.search(r'([^.]+)\.([^.]+)\.', file_path.split('/')[-1])
        bolt_mode = match.group(1)
        run_type = match.group(2)
 
    data = spark.sparkContext.textFile(file_path).collect()
 
    pattern = re.compile(r"\[ (\d+)s \].*?tps: ([\d.]+)")

    results = []
    avg_tps = []

    for line in data:
        matches = pattern.findall(line)
        results.extend(matches)
    
        avg_matches = tps_re.findall(line)
        avg_tps.extend(avg_matches)

    test_runs = []
    current_run = []
    current_second = 1
 
    for second, tps in results:
        second = int(second)
        tps = float(tps)
 
        if second == 1 and current_run:
            test_runs.append(current_run)
            current_run = []
 
        current_run.append((second, tps))
 
    if current_run:
        test_runs.append(current_run)
 
    Path = spark._jvm.org.apache.hadoop.fs.Path
    fs = spark._jvm.org.apache.hadoop.fs.FileSystem.get(spark.sparkContext._jsc.hadoopConfiguration())

    avg_values = []
    for i, run in enumerate(test_runs, start=1):
            seconds, tps_values = zip(*run)
            plt.figure()
            plt.plot(seconds, tps_values, label=f"Run {i}", color='r')

            avg_value = float(avg_tps[i - 1])
                
            y_min, y_max = plt.ylim()
 
            avg_value = float(avg_tps[i - 1])
            y_min = min(y_min, avg_value - 5)
            y_max = max(y_max, avg_value + 5)
            plt.ylim(y_min, y_max)
    

            plt.axhline(y=avg_value, color='b', linestyle='--', label='Average TPS')
    
            y_ticks = plt.yticks()[0]
    
            tolerance = (y_max - y_min) * 0.05
            modified_ticks = list(y_ticks)
            if any(abs(avg_value - tick) < tolerance for tick in y_ticks):
                modified_ticks = [avg_value if abs(avg_value - tick) < tolerance else tick for tick in y_ticks]
            else:
                modified_ticks.append(avg_value)
    
            plt.yticks(sorted(modified_ticks))
            
            plt.title(f"TPS Trend for Test Run {i} - {bolt_mode} - {run_type}")
            plt.xlabel("Time (seconds)")
            plt.ylabel("Transactions per Second (TPS)")
            plt.legend()
            plt.tight_layout()

            output_file = f"{output_dir}{bolt_mode}_{run_type}_tps_trend_{i}.png"
            output_path = Path(output_file)
            # plt.savefig(output_file, format="png")
            # plt.close()
            
            buffer = io.BytesIO()
            plt.savefig(buffer, format='png')
            plt.close()
            buffer.seek(0)
            data_bytes = buffer.read()
            output_stream = fs.create(output_path, True)
            output_stream.write(bytearray(data_bytes))
            output_stream.close()

            avg_values.append(avg_value)

    return avg_values


def process(spark, file_list, csv_output, img_path):
    """ Process all files in the directory, handle plot and logging. """
    csv_data = []
    for file_path in file_list:
        file_name = file_path.split('/')[-1]
        run_type = file_name.split('.')[1]
        mode = file_name.split('.')[0]
        avg_tps_values = plot_tps_trend(spark, file_path, img_path, mode, run_type)    
        average = sum(avg_tps_values) / len(avg_tps_values)
        csv_data.append((mode, run_type, average))
    ## write to csv
    columns = ["Mode", "RunType", "AverageTPS"]
    df = spark.createDataFrame(csv_data, schema=columns)
    df.coalesce(1).write.mode("overwrite").option("header", "true").csv(csv_output)


