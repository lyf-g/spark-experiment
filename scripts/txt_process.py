import os
import re
import csv
import matplotlib.pyplot as plt

def plot_tps_trend(file_path, output_dir, bolt_mode=None, run_type=None):
    """ Plot the TPS trends for given runs within a file. """
    tps_re = re.compile(r"transactions:\s+\d+\s+\((\d+\.\d+) per sec\.\)")
    if bolt_mode is None or run_type is None:
        match = re.search(r'([^.]+)\.([^.]+)\.', file_path)
        bolt_mode = match.group(1)
        run_type = match.group(2)
 
    with open(file_path, "r") as file:
        data = file.read()
 
    pattern = re.compile(r"\[ (\d+)s \].*?tps: ([\d.]+)")
    results = pattern.findall(data)
    avg_tps = tps_re.findall(data)
 
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

            output_file = os.path.join(output_dir, f"{bolt_mode}_{run_type}_tps_trend_{i}.png")
            plt.savefig(output_file, format="png")
            plt.close()
            
            avg_values.append(avg_value)

    return avg_values


def process_files(input_dir, output_dir, csv_output):
    """ Process all files in the directory, handle plot and logging. """
    files = [f for f in os.listdir(input_dir) if f.endswith('.sysbench.txt')]
    with open(csv_output, 'a', newline='') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow(['Mode', 'Run Type', 'Average TPS'])

        for file_index, file in enumerate(files):
            file_path = os.path.join(input_dir, file)
            mode, run_type = file.split('.')[:2]
            avg_tps_values = plot_tps_trend(file_path, output_dir, mode, run_type)
            
            average = sum(avg_tps_values) / 5
            csv_writer.writerow([mode, run_type, average])


if __name__ == '__main__':
    data_directory = './data/script_output'
    img_directory = './imgs'
    csv_file_path = './csvs/mysqld_tps.csv'
    os.makedirs(img_directory, exist_ok=True)
    os.makedirs(os.path.dirname(csv_file_path), exist_ok=True)
    process_files(data_directory, img_directory, csv_file_path)

    print('Data processing complete.')