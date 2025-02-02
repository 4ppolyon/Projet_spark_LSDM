from pyspark import SparkContext, SparkConf, RDD
import matplotlib.pyplot as plt
import sys
import os

# Si vous avez un message d'erreur vous disant que la library src n'existe pas, décommentez la ligne suivante :
# sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

# Importer les modules
from src.questions import *  # Ne pas inclure "src." ici

# Create a Spark Context
conf = (
    SparkConf()
    .setAppName("Spark Project")
    .setMaster("local[*]") # local[*] to use all the cores of the CPU
    .set("spark.sql.shuffle.partitions", "200")  # Limit shuffle partitions
    .set("spark.shuffle.file.buffer", "128k")  # Adjust buffer size
    .set("spark.shuffle.manager", "sort")  # Use the sort shuffle manager
    .set("spark.local.dir", "/tmp/spark-temp")  # Ensure sufficient temp space
    .set("spark.shuffle.compress", "true")  # Enable shuffle compression
    .set("spark.rdd.compress", "true")  # Enable compression for RDDs
    .set("spark.shuffle.spill.compress", "true")  # Enable spill compression
    .set("spark.io.compression.codec", "lz4")  # Use LZ4 for better compression
)
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")

# affiliate the col list below
machine_event_col = ['timestamp', 'machineID', 'eventtype', 'platformID', 'cpucapacity', 'memorycapacity']
job_event_col = ['timestamp', 'missinginfo', 'jobID', 'event_type', 'username', 'scheduling_class', 'job_name', 'logicaljobname']
task_event_col = ['timestamp', 'missinginfo', 'jobID', 'task_index', 'machineID', 'event_type', 'username', 'scheduling_class', 'priority', 'cpu', 'ram', 'disk', 'machineconstraint']
task_usage_col = ['start_time', 'end_time', 'jobID', 'task_index', 'machineID', 'cpu_rate', 'canonical_memory', 'assigned_memory', 'unmapped_page_cache', 'total_page_cache', 'max_memory', 'disk_io_time', 'local_disk_space', 'max_disk_space', 'max_cpu_rate', 'max_disk_io_time', 'cpi', 'mai', 'sampling_rate', 'aggregation_type', 'sampled_cpu_usage']

def split_data(data):
    return data.map(lambda x: x.split(','))

def load_data(name_folder, name_file="*")->RDD[str]:
    # concatenate the name with ./data/ and /*.csv
    data = sc.textFile(f"./data/{name_folder}/{name_file}.csv")
    # print(data.count(), "lines loaded from", name_folder)
    return split_data(data)

#####################
#                   #
# Main Execution    #
#                   #
#####################

def q1():
    print("_" * 100,"\nQuestion 1 :")
    print("What is the distribution of the machines according to their CPU capacity?\n")
    print("Loading machine_events")
    data = load_data("machine_events")
    print()

    start = time.time()
    res1 = question1(data, machine_event_col)
    print("\nExecution Time :", round(time.time() - start, 2), "s\n")

    # plot the data
    x = [x[0] for x in res1]
    y = [x[1] for x in res1]
    plt.bar(x, y)
    plt.xlabel('CPU Capacity')
    plt.ylabel('Number of Machines')
    if len(sys.argv) > 1 and sys.argv[1] == "filtered":
        plt.title('Distribution of Machines according to their CPU Capacity (Filtered Data)')
    else:
        plt.title('Distribution of Machines according to their CPU Capacity')
    plt.show()

def q2():
    print("_" * 100, "\nQuestion 2 :")
    print("What is the percentage of computational power lost due to maintenance?\n")
    print("Loading machine_events")
    data = load_data("machine_events")
    print()

    start = time.time()
    res2 = question2(data, machine_event_col)
    if res2 == -1:
        print("An error occurred, please check the data.")
    else:
        print("Percentage of computational power lost due to maintenance : ", round(res2, 2), "%")
    # Temps d'exécution
    print("\nExecution Time :", round(time.time() - start, 2), "s\n")

def q3():
    print("_" * 100,"\nQuestion 3 :")
    print("What is the distribution of the number of jobs/tasks per scheduling class?\n\n"
          "Computing Jobs per scheduling class\n"
          "Loading job_events")
    TimeJob = time.time()
    data_job = load_data("job_events")
    # data_job = load_data("job_events", "part-00000-of-00500")
    jobs = question3_job(data_job, job_event_col)
    TimeJob = round(time.time() - TimeJob, 2)
    print("\nJobs per scheduling class :" 
            "\n   - Class 0 :", jobs[0][1],"jobs (",jobs[0][2],"% )"
            "\n   - Class 1 :", jobs[1][1],"jobs (",jobs[1][2],"% )"
            "\n   - Class 2 :", jobs[2][1],"jobs (",jobs[2][2],"% )"
            "\n   - Class 3 :", jobs[3][1],"jobs (",jobs[3][2],"% )"
            "\n\nExecution Time for Jobs :", TimeJob, "s\n")

    print("Computing Tasks per scheduling class\n"
          "Loading task_events")
    TimeTask = time.time()
    data_task = load_data("task_events")
    # data_task = load_data("task_events", "part-00000-of-00500")
    tasks = question3_task(data_task, task_event_col)
    TimeTask = round(time.time() - TimeTask, 2)
    print("\nTasks per scheduling class :"
            "\n   - Class 0 :", tasks[0][1],"tasks (",tasks[0][2],"% )"
            "\n   - Class 1 :", tasks[1][1],"tasks (",tasks[1][2],"% )"
            "\n   - Class 2 :", tasks[2][1],"tasks (",tasks[2][2],"% )"
            "\n   - Class 3 :", tasks[3][1],"tasks (",tasks[3][2],"% )"
            "\n\nExecution Time for Tasks :", TimeTask, "s\n")

    # Temps d'exécution
    print("Total Execution Time :", round(TimeJob + TimeTask, 2), "s\n")

    # Création des sous-graphiques
    fig, axs = plt.subplots(1, 2, figsize=(12, 5))
    fig.suptitle('Distribution of the number of jobs/tasks per scheduling class')

    # Premier sous-graphe
    x_jobs = [x[0] for x in jobs]
    y_jobs = [x[1] for x in jobs]
    percent_jobs = [x[2] for x in jobs]

    ax1 = axs[0]
    ax1.bar(x_jobs, y_jobs, color='skyblue')
    ax1.set_title('Jobs per scheduling class')
    ax1.set_xlabel('Scheduling Class')
    ax1.set_ylabel('Number of Jobs (Empirical)', color='blue')

    # Axe secondaire pour le premier sous-graphe
    ax1_secondary = ax1.twinx()
    ax1_secondary.plot(x_jobs, percent_jobs, color='orange', marker='o')
    ax1_secondary.set_ylabel('Percentage (%)', color='orange')

    # Deuxième sous-graphe
    x_tasks = [x[0] for x in tasks]
    y_tasks = [x[1] for x in tasks]
    percent_tasks = [x[2] for x in tasks]

    ax2 = axs[1]
    ax2.bar(x_tasks, y_tasks, color='lightgreen')
    ax2.set_title('Tasks per scheduling class')
    ax2.set_xlabel('Scheduling Class')
    ax2.set_ylabel('Number of Tasks (Empirical)', color='green')

    # Axe secondaire pour le deuxième sous-graphe
    ax2_secondary = ax2.twinx()
    ax2_secondary.plot(x_tasks, percent_tasks, color='red', marker='o')
    ax2_secondary.set_ylabel('Percentage (%)', color='red')

    # Affichage des graphiques
    plt.tight_layout(rect=(0.0, 0.03, 1.0, 0.95))
    plt.show()



def q4():
    print("_" * 100,"\nQuestion 4 :")
    print("Do tasks with a low scheduling class have a higher probability of being evicted?")
    print("Loading task_events")
    start = time.time()
    data = load_data("task_events")
    question4(data, task_event_col)
    print("\nExecution Time :", round(time.time() - start, 2), "s\n")

def q5():
    print("_" * 100,"\nQuestion 5 :")
    print("In general, do tasks from the same job run on the same machine?")
    print("Loading task_events")
    start = time.time()
    data = load_data("task_events")
    question5(data, task_event_col)
    print("\nExecution Time :", round(time.time() - start, 2), "s\n")

def q6():
    print("_" * 100,"\nQuestion 6 :")
    print("Are the tasks that request the more resources the one that consume the more resources?")
    print("Loading task_events")
    start = time.time()
    data = load_data("task_events")
    # data = load_data("task_events","part-00000-of-00500")
    print("Loading task_usage")
    data_usage = load_data("task_usage")
    # data_usage = load_data("task_usage","part-000??-of-00500")
    question6(data, data_usage, task_event_col, task_usage_col)
    print("\nExecution Time :", round(time.time() - start, 2), "s\n")

def q7():
    print("_" * 100,"\nQuestion 7 :")

def custom():
    print("_" * 100,"\nCustom Question : Which machine has the highest/lowest ratio failure/task?")
    print("Loading task_events")
    start = time.time()
    data = load_data("task_events")
    custom1(data, task_event_col)
    print("\nExecution Time :", round(time.time() - start, 2), "s\n")

def custom2():
    print("_" * 100,"\nCustom Question 2 :")

def runall():
    q1()
    q2()
    q3()
    q4()
    q5()
    q6()
    q7()
    custom()
    custom2()

questions = {
    "1": q1,
    "2": q2,
    "3": q3,
    "4": q4,
    "5": q5,
    "6": q6,
    "7": q7,
    "custom": custom,
    "custom2": custom2
}

if len(sys.argv) > 1 and [x for x in questions if x in sys.argv]:
    for x in [x for x in questions if x in sys.argv]:
        questions[x]()
else:
    runall()