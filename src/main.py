import time
from pyspark import SparkContext
import matplotlib.pyplot as plt
from src.questions import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# affiliate the col list below
machine_event_col = ['timestamp', 'machineID', 'eventtype', 'platformID', 'cpucapacity', 'memorycapacity']
job_event_col = ['timestamp', 'missinginfo', 'jobID', 'eventtype', 'user', 'schedulingclass', 'jobname', 'logicaljobname']
task_event_col = ['timestamp', 'missinginfo', 'jobID', 'taskindex', 'machineID', 'eventtype', 'username', 'schedulingclass', 'priority', 'cpu', 'ram', 'disk', 'machineconstraint']

def split_data(data):
    data = data.map(lambda x: x.split(','))
    print("Data Loaded", data.count(), "rows")
    return data

# split the data
def filter_data(data):
    print("Filtering Data to remove rows with missing values (if you want to keep them, do not use the argument 'filtered')")
    # filter the data to remove the rows with missing values
    data = data.filter(lambda x: all(c != '' for c in x))
    print("Data Filtered", data.count(), "rows remaining")
    return data

def load_data(name):
    # concatenate the name with ./data/ and /*.csv
    data = sc.textFile(f"./data/{name}/*.csv")
    data = split_data(data)
    # if we execute the code with the argument "filtered", we will filter the data to remove the rows with missing values
    if len(sys.argv) > 1 and "filtered" in sys.argv:
        data = filter_data(data)
    return data

#####################
#                   #
# Main Execution    #
#                   #
#####################

def q1():
    data = load_data("machine_events")
    print("_" * 100,"\nQuestion 1 :")
    print("What is the distribution of the machines according to their CPU capacity?\n")
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
    data = load_data("machine_events")
    print("_" * 100, "\nQuestion 2 :")
    print("What is the percentage of computational power lost due to maintenance?\n")

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

def q4():
    print("_" * 100,"\nQuestion 4 :")

def q5():
    print("_" * 100,"\nQuestion 5 :")

def runall():
    q1()
    q2()
    q3()
    q4()
    q5()

questions = {
    "1": q1,
    "2": q2,
    "3": q3,
    "4": q4,
    "5": q5
}

if len(sys.argv) > 1 and [x for x in questions if x in sys.argv]:
    for x in [x for x in questions if x in sys.argv]:
        questions[x]()
else:
    runall()