import sys
import time
from pyspark import SparkContext
import matplotlib.pyplot as plt
from src.questions import *

# start spark with 1 worker thread
sc = SparkContext("local[1]")
sc.setLogLevel("ERROR")

# import the data (no header)
data = sc.textFile("./data/machine_events/*.csv")

# split the data
data = data.map(lambda x: x.split(','))
print("Data Loaded", data.count(), "rows\n")

# affiliate the col list below
col = ['timestamp', 'machineID', 'eventtype', 'platformID', 'cpucapacity', 'memorycapacity']

# if we execute the code with the argument "filtered", we will filter the data to remove the rows with missing values
if len(sys.argv) > 1 and sys.argv[1] == "filtered":
    print("Filtering Data to remove rows with missing values (if you want to keep them, do not use the argument 'filtered')")
    data = data.filter(lambda x: all(c != '' for c in x))
    print("Data Filtered", data.count(), "rows remaining\n")

#####################
#                   #
# Main Execution    #
#                   #
#####################

start = time.time()
res1 = question1(data, col)
print("\nExecution Time :", round(time.time() - start, 2), "s\n")

# plot the data
x = [x[0] for x in res1]
y = [x[1] for x in res1]
plt.bar(x, y)
plt.xlabel('CPU Capacity')
plt.ylabel('Number of Machines')
if sys.argv[1] == "filtered":
    plt.title('Distribution of Machines according to their CPU Capacity (Filtered Data)')
else:
    plt.title('Distribution of Machines according to their CPU Capacity')
plt.show()
