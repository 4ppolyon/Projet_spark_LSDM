from src.utils import *

# Has a machine always the same cpucapacity?
# We do not consider machines that do not have a cpucapacity
# If we want to consider them, we can remove the filter (line 49)
def question0(data, c):
    # find the index of cpucapacity
    index = find_col(c, 'cpucapacity')

    # map the data to (machineID, cpucapacity)
    d = data.map(lambda x: (x[find_col(c, 'machineID')], x[index]))

    # reduce by key and check if all values are the same
    d = d.reduceByKey(lambda x, y: x if x == y else 'different')

    # collect and print results
    d = d.collect()
    diff = 0
    for line in d:
        if line[1] == 'different':
            # print("Machine", line[0], "has different CPU Capacity")
            diff+=1
    if diff > 0:
        print("There are", diff, "machines that have different CPU Capacity.")
    else:
        print("All machines have different CPU Capacity.")

# What is the distribution of the machines according to their CPU capacity?
def question1(data, c):
    print("What is the distribution of the machines according to their CPU capacity?")
    print("Fist we want to know if a machine can have different CPU Capacity")

    # check if a machine can have different CPU Capacity
    question0(data, c)

    print("\nNow we will display the distribution of the machines according to their CPU capacity\n")

    # find the index of cpucapacity
    index = find_col(c, 'cpucapacity')

    # map the data to (cpucapacity, 1)
    d = data.map(lambda x: (x[index], 1))

    # reduce the data by key
    d = d.reduceByKey(lambda x, y: x + y)

    # sort the data by key
    d = d.sortByKey()

    # print the data
    print("CPU Capacity Distribution :\n")
    d = d.collect()
    for line in d:
        if line[0] == '':
            print(line[1], "rows have no CPU Capacity", "(", round(line[1] / data.count() * 100, 2), "%)")
        else:
            # print the data in %
            print(line[1], "rows have CPU Capacity of", line[0], "(", round(line[1] / data.count() * 100, 2), "%)")

    return d