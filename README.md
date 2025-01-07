
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
There is only one file in the `machine_events` directory. This file contains the following columns:

| Column | Title           | Type        | Mandatory | Description                                                                                   | Example                                      |
|--------|-----------------|-------------|-----------|-----------------------------------------------------------------------------------------------|----------------------------------------------|
| 1      | Timestamp       | INTEGER     | YES       | Temps de l'événement en microsecondes depuis 600 secondes avant le début de la trace.         | 0                                            |
| 2      | Machine ID      | INTEGER     | YES       | Identifiant unique de la machine impliquée dans l'événement.                                  | 5, 6, etc.                                   |
| 3      | Event Type      | INTEGER     | YES       | Type d'événement : <br>0: ADD (ajout) <br>1: REMOVE (suppression) <br>2: UPDATE (mise à jour) | 0 (ADD)                                      |
| 4      | Platform ID     | STRING_HASH | NO        | Identifiant haché de la plateforme, correspondant à la microarchitecture et au chipset.       | HofLGzk1Or/8Ildj2+Lqv0UGGvY82NLoni8+J/Yy0RU= |
| 5      | CPU Capacity    | FLOAT       | NO        | Capacité normalisée du CPU de la machine (1.0 représente la capacité maximale).               | 0.5 (50% de la capacité maximale)            |
| 6      | Memory Capacity | FLOAT       | NO        | Capacité normalisée de la mémoire de la machine (1.0 représente la capacité maximale).        | 0.2493 (environ 25% de la capacité maximale) |

### job_events
There are 500 files in the `job_events` directory. Each file contains the following columns:

| Column | Title            | Type        | Mandatory | Description                                                                                                                                                       | Example      |
|--------|------------------|-------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|--------------|
| 1      | Timestamp        | INTEGER     | YES       | Temps de l'événement en microsecondes depuis 600 secondes avant le début de la trace.                                                                             | 0            |
| 2      | Missing Info     | INTEGER     | YES       | Indique si certaines informations sont manquantes dans l'événement : <br>0: non manquant <br>1                                                                    |              |
| 3      | Job ID           | INTEGER     | YES       | Identifiant unique de l'emploi impliqué dans l'événement.                                                                                                         | 5, 6, etc.   |
| 4      | Event Type       | INTEGER     | YES       | Type d'événement : <br>0: SUBMIT (soumission) <br>1: SCHEDULE (planification) <br>2: EVICT (éviction) <br>3: FAIL (échec) <br>4: FINISH (fin) <br>5: KILL (arrêt) | 0 (SUBMIT)   |
| 5      | User             | STRING_HASH | NO        | Identifiant haché de l'utilisateur qui a soumis l'emploi.                                                                                                         | 0L6+Gv3Bw6c= |
| 6      | Scheduling Class | INTEGER     | NO        | Classe de planification de l'emploi : <br>0: non défini <br>1: normale <br>2: best effort <br>3: background                                                       | 1 (normale)  |
| 7      | Job Name         | STRING_HASH | NO        | Nom de l'emploi.                                                                                                                                                  | 0L6+Gv3Bw6c= |
| 8      | Logical Job Name | STRING_HASH | NO        | Nom logique de l'emploi.                                                                                                                                          | 0L6+Gv3Bw6c= |

### task_events
There are 500 files in the `task_events` directory. Each file contains the following columns:

| Column | Title             | Type        | Mandatory | Description                                                                                                                                                       | Example                                      |
|--------|-------------------|-------------|-----------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------|----------------------------------------------|
| 1      | Timestamp         | INTEGER     | YES       | Temps de l'événement en microsecondes depuis 600 secondes avant le début de la trace.                                                                             | 0                                            |
| 2      | Missing Info      | INTEGER     | YES       | Indique si certaines informations sont manquantes dans l'événement : <br>0: non manquant <br>1                                                                    |                                              |
| 3      | Job ID            | INTEGER     | YES       | Identifiant unique de l'emploi impliqué dans l'événement.                                                                                                         | 5, 6, etc.                                   |
| 4      | Task Index        | INTEGER     | YES       | Index de la tâche dans l'emploi.                                                                                                                                  | 0, 1, etc.                                   |
| 5      | Machine ID        | INTEGER     | YES       | Identifiant unique de la machine impliquée dans l'événement.                                                                                                      | 5, 6, etc.                                   |
| 6      | Event Type        | INTEGER     | YES       | Type d'événement : <br>0: SUBMIT (soumission) <br>1: SCHEDULE (planification) <br>2: EVICT (éviction) <br>3: FAIL (échec) <br>4: FINISH (fin) <br>5: KILL (arrêt) | 0 (SUBMIT)                                   |
| 7      | User              | STRING_HASH | NO        | Identifiant haché de l'utilisateur qui a soumis l'emploi.                                                                                                         | 0L6+Gv3Bw6c=                                 |
| 8      | Scheduling Class  | INTEGER     | NO        | Classe de planification de la tâche : <br>0: non défini <br>1: normale <br>2: best effort <br>3: background                                                       | 1 (normale)                                  |
| 9      | Priority          | INTEGER     | NO        | Priorité de la tâche.                                                                                                                                             | 0                                            |
| 10     | CPU Request       | FLOAT       | NO        | Nombre de cœurs de CPU demandés par la tâche.                                                                                                                     | 0.5 (0.5 cœur de CPU)                        |
| 11     | Memory Request    | FLOAT       | NO        | Quantité de mémoire demandée par la tâche.                                                                                                                        | 0.2493 (environ 25% de la capacité maximale) |
| 12     | Disk Request      | FLOAT       | NO        | Quantité de disque demandée par la tâche.                                                                                                                         | 0.0 (0.0 disque)                             |
| 13     | Different Machine | INTEGER     | NO        | Indique si la tâche doit être exécutée sur une machine différente de celle où elle a été soumise : <br>0: non <br>1: oui                                          | 0 (non)                                      |

## Description of the analyses

We say here that we consider both filtered and unfiltered data. This is why we have two different histograms for the first question for example.

### Question 1
For this question we started by mapping the data as a key-value pair where the key the machine id and the value is 1.
We then reduced the data by summing the values for each key. This way we can count the number of machines.
We then print the number of machines.

After that, we mapped the data as a key-value pair where the key is the machine id and the value is the cpu capacity.
We suppressed the duplicates pairs by using the `distinct` function.
Then we mapped the data as a key-value pair where the key is the cpu capacity and the value is 1 for each machine.
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
