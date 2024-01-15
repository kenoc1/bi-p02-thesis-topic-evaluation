# bi-p02-thesis-topic-evaluation

# Init
```bash
docker compose up -d
```

# Database Schema + Creation
The dataset are dicts for every day and contains a json with two relevant json-files:
- db-topics
  - titel
  - abschlussarbeitstyp
  - studiengaenge
  - art_der_arbeit;ansprechpartner
  - status;erstellt
  - action;topic_id
  - url_topic_details
- db-topics-additional
  - titel
  - beschreibung
  - heimateinrichtung
  - art_der_arbeit
  - abschlussarbeitstyp
  - autor
  - status
  - aufgabenstellung
  - erstellt
  - topic_id
  - voraussetzung


I have create following database schema:
![DatabaseSchema.png](media%2FDatabaseSchema.png)

For the creation to the database i used a DDL-SQL-Script:
[DDL-SQL-File](create.sql)

# ETL: Importer
For the ETL process i have a python-script: [importer.py](importer.py)

For exceution:
```bash
python3 -m venv .venv
source .venv/bin/activate
pip3 install -r requirements.txt
python3 importer.py
```

Data cleaning:
Same data items for date 24.05.2023 on different pages where based on crawler issuer or Stud.Ip problem.
To solve this, execute [CLEANING-SQL-File](cleaning.sql). This will overwrite the date 24.05.2023 with the data from date 23.05.2023.

# Visualisation
Go to [localhost:3000](localhost:3000), create an Account and log in to metabase.
> **_NOTE:_**  use host.docker.internal instead of localhost for the host

I added a dashboard for the tasks/KPIs. The sql statements are in the file [tasks.sql](tasks.sql)

## Tasks
- T1 - How many thesis topics are published in a week, in a month, in a year?
- T2 - Which supervisor has the most thesis topics to offer?
- T3 - Which department has the most thesis topics to offer?
- T4 - How many thesis topics are "removed from the list" in a week, in a month, in a year?
- T5 - Create 1 task/business question of your own and answer this question: Which Thesis Type is Most Common?


## KPIs
- KPI 1 - Unique thesis topics published every month
- KPI 2 - Average thesis topics for each department
- KPI 3 - Create 1 KPI of your own, describe how to calculate it and put it on the dashboard: This KPI gives an overview of the distribution of thesis topics across different thesis types.

[Dashboard.pdf](media%2FDashboard.pdf)