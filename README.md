# Automated ETL Data Pipeline for Sparkify

## Project overview
Sparkify, a music streaming company, needs to automate and monitor data flows to their AWS Redshift data warehouse. The pipelines need to be dynamic, built from reusable tasks, monitored and allow for easy backfills. Another requirement is data quality component for the ETL procedures. Since their analytics department depends on the quality of the input, procedures must me implemented to allow for easy-to-execute tests to catch any discrepancies in the datsets as they arrive in the warehouse. 


### Input data

The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

### Project scope

Design an automated ETL data pipeline, create a set of custom Apache Airflow operators for:
- staging data, 
- filling the data warehouse, 
- running checks on the data


Initial SQL queries defining a set of staging, fact and dimension tables are provided. But there is a need to optimize a set of provided SQL queries to ensure data consistency, and allow appending to or removing existing tables.

---
## Project structure

```
.
├── dags
│   ├── create_tables.sql
│   └── sparkify.py
├── imgs
├── plugins
│   ├── helpers
│   │   ├── __init__.py
│   │   └── sql_queries.py
│   ├── __init__.py
│   ├── operators
│   │   ├── data_quality.py
│   │   ├── __init__.py
│   │   ├── load_dimension.py
│   │   ├── load_fact.py
│   │   └── stage_redshift.py
└── README.md 

```

- `dags/sparkify.py`: The main DAG defining the ETL processes, with its parameters and task ordering and dependencies
- `dags/create_tables.sql`: A set of SQL queries defining staging, dimensional and fact tables

- `plugins/helpers/sql_queries.py`: a set of helper SQL queries to:
		- extract information for staging tables 
        - define SQL test cases for data consistency operators
- `operators/*`: a set of  custom Airflow Operators 
	- `stage_redshift.py` : stages data from provided S3 bucket in JSON format into a Postgres database (or Amazon Redshift) to defined staging table 
	- `load_dimension.py` : loads data from a defined staging table into specified dimensional table
	- `load_fact.py`	  : loads data from a defined staging table into specified fact table
	- `data_quality.py`   : performs a set of defined tests consisting of a tuple (SQL QUERY, TEST). See the operator for details
    
    	- **Data test example**
        ```
        test = [    SqlQueries.songplays_check_nulls ,  "{}[0][0] == 0" ]

        ```
        
        First argument refers to pre-defined tests that counts the number of defined columns that contain `NULL` values. Second argument is the test that is first formatted, and in the place of curly brackets `{}` the results of the query is put, and then evaluated. In this example we expect the result to be 1x1, i.e. a single integer, therefore we select first entry of the first column (the only value) and permform an equality test `==` to zero. Test passes then if there are no null values.
  
**Note** all `__init__.py` files are specified in accordance to Apache Airflow documentation for custom plugins. The details are not discussed here.

---
## ETL Pipeline

### Running the pipeline:

1. Start Apache Airflow server:
	usually by executing on an dedicated Airflow Webserver
	`/opt/airflow/start.sh`
2. Specify credentials and connections to the warehouse in Airflow UI:
	1. Connection ID: `redshift` is a Postgres hook for a datawarehouse
    2. AWS credentials need to be stored in `aws_credentials` connection ID
    
3. Tweak pipeline setting in `dags/sparkify.py` to match the desired run parameters, among many:
	- [`schedule_interval`](https://airflow.apache.org/docs/stable/scheduler.html#dag-runs): how often to run the pipeline
    - `retries` - number of retries before failing a task
    - `retry_delay` - a delay before retries
    - `max_active_runs`: number of parallel tasks to execute at once
    - ... 
    
### Pipeline DAG

A Pipeline DAG is shown below
<img src="imgs/etl_dag.png" alt="DAG of a defined ETL pipeline in Airflow" width="800" height="300" border="10" />
 
