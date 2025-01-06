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

# Calculer le downtime et la puissance perdue
def calculate_downtime(events):
    begin = 0
    end = 0
    tmp = 0
    latence = 0
    iter = 0
    capa = 0

    for timestamp, event_type, cpucapacity in events:
        if iter == 0:
            if event_type == 1:
                begin = 0
                tmp = timestamp
            elif event_type == 0:  # add
                begin = timestamp
                latence += timestamp
        else:
            if event_type == 1:  # remove
                tmp = timestamp
            elif event_type == 0:  # add
                latence += timestamp - tmp
        end = timestamp
        capa = max(capa, cpucapacity)
        iter += 1

    if events:
        total_time = end - begin
        total_downtime = latence
        if total_time > 0:
            return total_downtime, total_time, capa
        else:
            return 0, 0, capa

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

def question2(data, c):
    # Indices des colonnes pertinentes
    timestamp_id = find_col(c, 'timestamp')
    eventtype_id = find_col(c, 'eventtype')
    machineID_id = find_col(c, 'machineID')
    cpucapacity_id = find_col(c, 'cpucapacity')

    # Filtrer les événements pertinents (add = 0, remove = 1)
    print("Filtering relevant events (add = 0, remove = 1)")
    events = data.filter(lambda x: x[eventtype_id] in ['0', '1'])

    # Mapper les données pour associer les événements à chaque machine
    print("Mapping data to associate events to each machine")
    events = events.map(
        lambda x: (x[machineID_id], (int(x[timestamp_id]), int(x[eventtype_id]), 0 if x[cpucapacity_id] == '' else float(x[cpucapacity_id]))))

    # Trier par machine et timestamp
    print("Sorting by machine and timestamp")
    events = events.groupByKey().mapValues(lambda x: sorted(list(x), key=lambda y: y[0]))

    # Calculating time losses for each machine
    print("Calculating time losses for each machine")
    results = events.mapValues(calculate_downtime)

    # Calculating total power lost and total power (multiplying the time lost by the CPU capacity)
    print("Calculating total power lost and total power (multiplying the time lost by the CPU capacity)")
    results = results.map(
        lambda x: (x[0], x[1][0], x[1][1], x[1][2], x[1][0] * x[1][2], x[1][1] * x[1][2]))
    total_power_lost = results.map(lambda x: x[4]).sum()
    total_power = results.map(lambda x: x[5]).sum()

    # Calculating the percentage of computational power lost due to maintenance
    if total_power == 0:
        print("Erreur : La puissance totale est nulle. Vérifiez les données d'entrée.")
        return -1
    percentage = total_power_lost / total_power * 100

    return percentage

# Calculate the distribution of jobs/tasks per scheduling class
def question3(data_job, data_task, job_event_col, task_event_col):
    # Map job events to (scheduling class, 1) and reduce to count per scheduling class
    scheduling_class_index_job = find_col(job_event_col, 'schedulingclass')
    job_name_index = find_col(job_event_col, 'jobname')

    # on va garder qu'une seule ligne par job
    data_job = data_job.map(lambda x: (x[job_name_index], x[scheduling_class_index_job]))
    data_job = data_job.distinct()
    job_number = data_job.count()

    # on va compter le nombre de job par scheduling class
    data_job = data_job.map(lambda x: (x[1], 1))
    data_job = data_job.reduceByKey(lambda a, b: a + b)
    data_job = data_job.collect()
    job_counts = data_job
    print("Number of jobs: ", job_number)

    # Map task events to (scheduling class, 1) and reduce to count per scheduling class
    scheduling_class_index_task = find_col(task_event_col, 'schedulingclass')
    job_name_index_task = find_col(task_event_col, 'jobID')
    task_index = find_col(task_event_col, 'taskindex')

    # On calcule la répartition des jobs
    job_repartition = []
    for scheduling_class, count in job_counts:
        job_repartition.append((scheduling_class, count, round(count / job_number * 100, 2)))

    # on va garder qu'une seule ligne par task
    data_task = data_task.map(lambda x: (x[job_name_index_task] + x[task_index], x[scheduling_class_index_task]))
    data_task = data_task.distinct()
    task_number = data_task.count()
    print("Number of tasks: ", task_number)

    # on va compter le nombre de task par scheduling class
    data_task = data_task.map(lambda x: (x[1], 1))
    data_task = data_task.reduceByKey(lambda a, b: a + b)
    data_task = data_task.collect()
    task_counts = data_task

    # On calcule la répartition des tasks
    task_repartition = []
    for scheduling_class, count in task_counts:
        task_repartition.append((scheduling_class, count, round(count / task_number * 100, 2)))

    # Sort the data
    job_repartition.sort(key=lambda x: x[0])
    task_repartition.sort(key=lambda x: x[0])
    print()

    return job_repartition, task_repartition



