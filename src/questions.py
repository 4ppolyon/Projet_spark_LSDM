import sys
import time
from src.utils import *

# Has a machine always the same cpucapacity?
# We do not consider machines that do not have a cpucapacity
def has_same_cpucapacity(data, c):
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

# Calculate the downtime of a machine and the total downtime
def calculate_downtime(events):
    begin = 0
    end = 0
    tmp = 0
    latency = 0
    i = 0
    capa = 0

    for timestamp, event_type, cpucapacity in events:
        if i == 0:
            if event_type == 1:
                begin = 0
                tmp = timestamp
            elif event_type == 0:  # add
                begin = timestamp
                latency += timestamp
        else:
            if event_type == 1:  # remove
                tmp = timestamp
            elif event_type == 0:  # add
                latency += timestamp - tmp
        end = timestamp
        capa = max(capa, cpucapacity)
        i += 1

    if events:
        total_time = end - begin
        total_downtime = latency
        if total_time > 0:
            return total_downtime, total_time, capa
        else:
            return 0, 0, capa

def are_undefined_scheduling_classes(data, cols):
    # Check if there are undefined scheduling classes
    index_sched_class = cols.index('scheduling_class')
    undefined_scheduling_classes = data.filter(lambda row: row[index_sched_class] == '').count()
    if undefined_scheduling_classes > 0:
        print(f'There are {undefined_scheduling_classes} undefined scheduling classes')
    else:
        print('There are no undefined scheduling classes')

def count_jobs(data, cols):
    # Find the index of the columns
    jobID_index = find_col(cols, 'jobID')
    t1 = time.time()
    x = data.map(lambda x: x[jobID_index]).distinct().count()
    print("We check that there are :", x, "jobs")
    print("Time to count the number of jobs:", time.time() - t1)
    return x

def count_tasks(data, cols):
    # Find the index of the columns
    jobID_index = find_col(cols, 'jobID')
    task_index = find_col(cols, 'task_index')
    t1 = time.time()
    x = data.map(lambda x: (x[jobID_index], x[task_index])).distinct().count()
    print("We check that there are :", x, "tasks")
    print("Time to count the number of tasks:", time.time() - t1)
    return x

# What is the distribution of the machines according to their CPU capacity?
def question1(data, c):
    print("Something to consider: We don't know if it is possible for a machine to have more than one CPU capacity.\n"
          "So we decided \"if a machine has more than one capacity, the machine is added for each capacity it has.\"")

    if len(sys.argv) > 1 and "check" in sys.argv:
        # check if a machine can have different CPU Capacity
        has_same_cpucapacity(data, c)
    else:
        print("\nWe made a script to check if a machine can have different CPU Capacity, you can run it by using the argument \"check\"\n"
              "/!\\ It will take more time to compute and display the name with all the CPU Capacity of each machine /!\\\n")


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

    # for each machine, we will keep the machineID and the cpucapacity
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

def safe_int(value):
    try:
        return int(value)
    except ValueError:
        return 0  # or any default value you want to use

def safe_float(value):
    try:
        return float(value)
    except ValueError:
        return 0.0  # or any default value you want to use

def question2(data, c):
    # Find the index of the columns
    timestamp_id = find_col(c, 'timestamp')
    eventtype_id = find_col(c, 'eventtype')
    machineID_id = find_col(c, 'machineID')
    cpucapacity_id = find_col(c, 'cpucapacity')

    # Filter relevant events (add = 0, remove = 1)
    print("Filtering relevant events (add = 0, remove = 1)")
    events = data.filter(lambda x: x[eventtype_id] in ['0', '1'])

    # Mapping data to associate events to each machine
    print("Mapping data to associate events to each machine")
    events = events.map(
        lambda x: (x[machineID_id], (safe_int(x[timestamp_id]), safe_int(x[eventtype_id]), safe_float(x[cpucapacity_id]))))

    # Grouping events by machine and sorting by timestamp
    print("Sorting by machine and timestamp")
    events = events.groupByKey().mapValues(lambda x: sorted(list(x), key=lambda y: y[0]))

    # Calculating time losses for each machine
    print("Calculating time losses for each machine")
    results = events.mapValues(calculate_downtime)

    # Collecting results
    print("Calculating total power lost and total power (multiplying the time lost by the CPU capacity)")
    total_power_lost, total_power = (
        # We map the results to extract the power lost and the total power
        results.map(
            lambda x: (x[1][0] * x[1][2], x[1][1] * x[1][2])
        )
        # We reduce the results to sum the power lost and the total power
        .reduce(
            lambda acc, value: (acc[0] + value[0], acc[1] + value[1])
        )
    )

    # Calcul du pourcentage
    if total_power == 0:
        print("Error: Total power is 0")
        return -1

    return total_power_lost / total_power * 100

# Calculate the distribution of jobs/tasks per scheduling class
def question3_job(data_job, job_event_col):

    if len(sys.argv) > 1 and "check" in sys.argv:
        are_undefined_scheduling_classes(data_job, job_event_col)
        nbjob = count_jobs(data_job, job_event_col)

    # Find indices
    try:
        scheduling_class_index_job = find_col(job_event_col, 'scheduling_class')
        job_name_index = find_col(job_event_col, 'jobID')
    except Exception as e:
        print(f"Error finding column indices: {e}")
        raise

    # Keep only one line per job with the scheduling class
    try:
        print("Keeping only one line per job")
        data_job = data_job.map(lambda x: (x[job_name_index], x[scheduling_class_index_job])).distinct()
        job_number = data_job.count()
        print("Number of jobs with scheduling class:", job_number)
        if len(sys.argv) > 1 and "check" in sys.argv:
            print("So there are", job_number - nbjob, "jobs which are not unique")
    except Exception as e:
        print(f"Error processing distinct job data: {e}")
        raise

    # Count the number of jobs per scheduling class
    try:
        print("\nCounting the number of jobs per scheduling class")
        job_counts = data_job.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()
        print(" - Ended")
    except Exception as e:
        print(f"Error counting jobs: {e}")
        raise

    # Compute the percentage of jobs per scheduling class
    try:
        print("Computing the percentage of jobs per scheduling class")
        job_repartition = []
        for scheduling_class, count in job_counts:
            job_repartition.append((scheduling_class, count, round(count / job_number * 100, 2)))
        print(" - Ended")
    except Exception as e:
        print(f"Error computing job percentages: {e}")
        raise

    # Sort the data
    try:
        print("Sorting data")
        job_repartition.sort(key=lambda x: x[0])
        print(" - Sorted")
    except Exception as e:
        print(f"Error sorting job data: {e}")
        raise

    return job_repartition

def question3_task(data_task, task_event_col):

    if len(sys.argv) > 1 and "check" in sys.argv:
        are_undefined_scheduling_classes(data_task, task_event_col)
        nbtask = count_tasks(data_task, task_event_col)

    # Find indices
    try:
        scheduling_class_index_task = find_col(task_event_col, 'scheduling_class')
        job_name_index_task = find_col(task_event_col, 'jobID')
        task_index = find_col(task_event_col, 'task_index')
    except Exception as e:
        print(f"Error finding column indices: {e}")
        raise

    # Keep only one line per task
    try:
        print("Keeping only one line per task")
        data_task = data_task.map(lambda x: (x[job_name_index_task] + x[task_index], x[scheduling_class_index_task])).distinct()
        task_number = data_task.count()
        print("Number of tasks with scheduling class:", task_number)
        if len(sys.argv) > 1 and "check" in sys.argv:
            print("So there are", task_number - nbtask, "tasks which are not unique")
    except Exception as e:
        print(f"Error processing distinct task data: {e}")
        raise

    # Count the number of tasks per scheduling class
    try:
        print("\nCounting the number of tasks per scheduling class")
        task_counts = data_task.map(lambda x: (x[1], 1)).reduceByKey(lambda a, b: a + b).collect()
        print(" - Ended")
    except Exception as e:
        print(f"Error counting tasks: {e}")
        raise

    # Compute the percentage of tasks per scheduling class
    try:
        print("Computing the percentage of tasks per scheduling class")
        task_repartition = []
        for scheduling_class, count in task_counts:
            task_repartition.append((scheduling_class, count, round(count / task_number * 100, 2)))
        print(" - Ended")
    except Exception as e:
        print(f"Error computing task percentages: {e}")
        raise

    # Sort the data
    try:
        print("Sorting data")
        task_repartition.sort(key=lambda x: x[0])
        print(" - Sorted")
    except Exception as e:
        print(f"Error sorting task data: {e}")
        raise

    return task_repartition

def question4(data, cols):
    ### Do tasks with a low scheduling class have a higher probability of being evicted?

    index_sched_class = cols.index('scheduling_class')
    index_event_type = cols.index('event_type')

    # Here, it is not clear what is considered a low scheduling class
    # We will consider the classes 0 and 1 as low scheduling classes but this can be changed easily by modifying the low_classes set
    low_classes = {'0', '1'}
    low_scheduling_tasks = data.filter(lambda row: row[index_sched_class] in low_classes)
    other_scheduling_tasks = data.filter(lambda row: row[index_sched_class] not in low_classes)

    # We compute the probability of being evicted for low scheduling tasks
    total_low_scheduling_tasks = low_scheduling_tasks.count()
    low_scheduling_tasks_evicted = low_scheduling_tasks.filter(lambda row : row[index_event_type] == '2').count()
    p1 = (low_scheduling_tasks_evicted / total_low_scheduling_tasks) * 100

    # We compute the probability of being evicted for other scheduling tasks
    total_other_scheduling_tasks = other_scheduling_tasks.count()
    other_scheduling_tasks_evicted = other_scheduling_tasks.filter(lambda row : row[index_event_type] == '2').count()
    p2 = (other_scheduling_tasks_evicted / total_other_scheduling_tasks) *100

    print(f'There is {p1:.2f}% chance that a task with a low scheduling and {p2:.2f}% chance that a task with a higher scheduling will be removed')
    print("Results:"
          "\nLow scheduling tasks:"
          "\nEvicted tasks:", low_scheduling_tasks_evicted,
          "\nTotal tasks:", total_low_scheduling_tasks,
          "\nOther scheduling tasks:"
          "\nEvicted tasks:", other_scheduling_tasks_evicted,
          "\nTotal tasks:", total_other_scheduling_tasks)
    print("It is",p1>p2,"that a task with a low scheduling class has a higher probability of being evicted")


def question5(data, cols): 
    # In general, do tasks from the same job run on the same machine?
    # We could interpret "In general" in different ways. 
    # We choose to give a threshold, here 75%, were if there is more than *threshold* jobs that run on the same machine we can answer the question affirmatively.
    threshold = 75 # value in percent 

    index_jobID = cols.index('jobID')
    index_machineID = cols.index('machineID')

    # filter out missing data 
    data = data.filter(lambda row: row[index_jobID] != '' and row[index_machineID] != '')

    # map relevant data, this will give a key-value pair (jobID, machineID) 
    job_machine_pairs = data.map(lambda row: (row[index_jobID], row[index_machineID]))

    # we are going to count the different machines for a given job. Since we count the lines for a job, we need to remove duplicates 
    job_machine_pairs = job_machine_pairs.distinct()

    # we group the values by keys, i.e. by jobID. 
    grouped_machines = job_machine_pairs.groupByKey()

    # if one job runs on only one machine, there should be only one value (jobID, machineID) for this job
    grouped_machines = grouped_machines.mapValues(len)

    # we count how many jobs run on one machine, and how many jobs run on multiple machines.
    jobs_single_machine = grouped_machines.filter(lambda x: x[1] == 1).count()
    jobs_multi_machine = grouped_machines.filter(lambda x: x[1] > 1).count()

    total_jobs = jobs_single_machine + jobs_multi_machine
    proportion_single_machine = (jobs_single_machine / total_jobs) * 100
    proportion_multi_machine = (jobs_multi_machine / total_jobs) * 100

    print(f"Proportion of jobs running on a single machine: {proportion_single_machine:.2f}%")
    print(f"Proportion of jobs running on multiple machines: {proportion_multi_machine:.2f}%")

    print("It is",proportion_single_machine>threshold,"that in general, tasks from the same job run on the same machine")

