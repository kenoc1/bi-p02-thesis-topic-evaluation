# bi-p02-thesis-topic-evaluation

## Task
### Starting Point
#### Review the given dataset
- make sure, that you can access the data
- check README.md
- study given dataset
- make sure, that you understand the value of the data and process behind the data
#### Create a data vault model for the given dataset
- identify events within the dataset (could be multiple)
- identify respected context information
- make sure that the schema reflects the data and no data get lost
#### Create a database schema, that is based on the designed schema model 
#### ETL
- Extract all relevant data fields from the given dataset Transform  extracted all relevant data
- Import transformed data into created database
#### Visualization/KPI
- design and calculate KPI to insure the import of the data was successful 
- perform tasks (see the section below)
- calculate KPI (see the section below)

### Tasks
- How many thesis topics are published in a week, in a month, in a year?
- Which supervisor has the most thesis topics to offer?
- Which department has the most thesis topics to offer?
- How many thesis topics are "removed from the list" in a week, in a month, in a year? Create 1 task/business question of your own and answer this question.

### KPIs
- Unique thesis topics published every month
- Average thesis topics for each department
- Create 1 KPI of your own, describe how to calculate it and put it on the dashboard


## Importer

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
