import sys
from src.utils import *

# Has a machine always the same cpucapacity?
# We do not consider machines that do not have a cpucapacity
def check(data, c):
    print("\nChecking if a machine can have different CPU Capacity")
    # Find the index of cpucapacity
    index = find_col(c, 'cpucapacity')
    machine_index = find_col(c, 'machineID')

    # Map the data to (machineID, cpucapacity)
    d = data.map(lambda x: (x[machine_index], x[index]))

    # Reduce by key to collect unique cpucapacities per machine
    d = d.groupByKey().mapValues(set)

    # Collect results
    d = d.collect()

    diff_machines = {}
    for machineID, cpucapacities in d:
        if len(cpucapacities) > 1:
            diff_machines[machineID] = list(cpucapacities)

    if diff_machines:
        print("List of the", len(diff_machines), "machines that have different CPU Capacities:")
        for machineID, cpucapacities in diff_machines.items():
            print(f"Machine ID {machineID} has CPU Capacities: {cpucapacities}")
    else:
        print("A machine always has the same CPU Capacity")
    print()


# What is the distribution of the machines according to their CPU capacity?
def question1(data, c):
    print("Something to consider: We don't know if it is possible for a machine to have more than one CPU capacity.\n"
          "So we decided \"if a machine has more than one capacity, the machine is added for each capacity it has.\"")

    if len(sys.argv) > 1 and "check" in sys.argv:
        # check if a machine can have different CPU Capacity
        check(data, c)
    else:
        print("\nWe made a script to check if a machine can have different CPU Capacity, you can run it by using the argument \"check\"\n"
              "/!\\ It will take more time to compute and display the name with all the CPU Capacity of each machine /!\\\n")


    nb_machines = 0
    print("Computing the distinct number of machines")
    # find the index of machineID
    machineID = find_col(c, 'machineID')

    # map the data to (machineID, 1)
    d = data.map(lambda x: (x[machineID], 1))

    # reduce the data by key
    d = d.reduceByKey(lambda x, y: x + y)

    # count the number of machines
    nb_machines = d.count()
    print("Number of machines: ", nb_machines)

    print("Computing the distribution of the machines according to their CPU capacity\n")

    # find the index of cpucapacity
    cpucapacity = find_col(c, 'cpucapacity')

    # pour chaque machine, on va créer un tuple (cpucapacity, 1) en sachant que on a plusieur ligne pour une machine
    d = data.map(lambda x: (x[machineID], x[cpucapacity]))

    # on va supprimer les doublons
    d = d.distinct()

    # on va compter le nombre de machine par cpucapacity
    d = d.map(lambda x: (x[1], 1)).reduceByKey(lambda x, y: x + y)

    # print the data
    print("CPU Capacity Distribution :")

    # sort by the cpucapacity
    d = d.sortByKey().collect()
    for line in d:
        if line[0] == '':
            print("   ", line[1], "row(s) have no CPU Capacity", "(" + str(round(line[1] / nb_machines * 100, 2)) + "%)")
        else:
            # print the data in %
            print("   ", line[1], "machine(s) can have a CPU Capacity of", line[0], "(" + str(round(line[1] / nb_machines * 100, 2)) + "%)")

    print("\nNote : We think that a machine cannot have different CPU capacity and if the sum of the percentage is not\n"
          "equal to 100%, it is because some machines do not have a CPU Capacity. You can see this yourself by summing\n"
          "the number of machines for all the CPU capacity and comparing it to the total number of machines.\n")

    return d