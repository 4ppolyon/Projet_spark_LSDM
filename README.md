
# Spark Project M2 MOSIG

## How to run the project
We assume that you have already installed Spark and that you have a working version of Python 3.10 or higher.
to run all the questions of the project, you need to run the following commands **in the root directory of the project**:
```bash
    python3 ./src/main.py
```
If you want to run with a filter on the missing values, you can run the following command:
```bash
    python3 ./src/main.py filtered
```
If you want to run a specific question, you can run the following command:
```bash
    python3 ./src/main.py <questions_number>
```
where `<questions_number>` are the number of the questions you want to run (1, 2, etc. separated by a space).
And you can obviously add the `filtered` argument and mix the arguments.

Example:
```bash
    python3 ./src/main.py 1 2 filtered
```

## Description of the data

### machine_events

| Column | Title           | Type        | Mandatory | Description                                                                                   | Example                                      |
|--------|-----------------|-------------|-----------|-----------------------------------------------------------------------------------------------|----------------------------------------------|
| 1      | Timestamp       | INTEGER     | YES       | Temps de l'événement en microsecondes depuis 600 secondes avant le début de la trace.         | 0                                            |
| 2      | Machine ID      | INTEGER     | YES       | Identifiant unique de la machine impliquée dans l'événement.                                  | 5, 6, etc.                                   |
| 3      | Event Type      | INTEGER     | YES       | Type d'événement : <br>0: ADD (ajout) <br>1: REMOVE (suppression) <br>2: UPDATE (mise à jour) | 0 (ADD)                                      |
| 4      | Platform ID     | STRING_HASH | NO        | Identifiant haché de la plateforme, correspondant à la microarchitecture et au chipset.       | HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU= |
| 5      | CPU Capacity    | FLOAT       | NO        | Capacité normalisée du CPU de la machine (1.0 représente la capacité maximale).               | 0.5 (50% de la capacité maximale)            |
| 6      | Memory Capacity | FLOAT       | NO        | Capacité normalisée de la mémoire de la machine (1.0 représente la capacité maximale).        | 0.2493 (environ 25% de la capacité maximale) |

## Description of the analyses

We say here that we consider both filtered and unfiltered data. This is why we have two different histograms for the first question for example.

### Question 1
For this question we started by mapping the data as a key-value pair where the key is the cpu capacity and the value is 1 for each machine.
We then reduced the data by summing the values for each key. This way we can count the number of machines for each cpu capacity.
We then print the distribution of the CPU capacity of the machines.
To fully answer this question, we plot the distribution of the CPU capacity with a histogram.

## Results

### Question 1
- The distribution of the CPU capacity of the machines is shown in the following histogram:
![CPU Capacity Histogram](./img/question1.png)
- The distribution of the CPU capacity of the machines (filtered) is shown in the following histogram:
![CPU Capacity Histogram (filtered)](./img/question1f.png)

### Question 2

### Author:
- Romain Alves
- Sylvain Joubert
