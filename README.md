# udacity-data-pipeline

## Purpose
Sparkify has grown their user base and song database and 
want to move their processes and data onto the cloud. 
The data resides in S3, in a directory 
of JSON logs on user activity on the app, as well as a 
directory with JSON metadata on the songs in their app.

This data pipeline extracts the data 
from S3, stages them in Redshift, and transforms data into 
a set of dimensional tables for the analytics team to 
continue finding insights in what songs their users are 
listening to. Data quality queries are executed after every
dimension table load.

## Content
airflow/dags/udac_example_dag.py
- main dag with all execution steps
  
airflow/dags/udag_subdag.py
- subdag definition for dimension table load and data quality checks

operators
- airflow/plugins/operators/data_quality.py
- airflow/plugins/operators/load_dimension.py
- airflow/plugins/operators/load_fact.py
- airflow/plugins/operators/stage_redshift.py

airflow/plugins/helpers/sql_queries.py
- sql statements for table inserts

redshift setup
- see requirements section

## Requirements
The tables need to be created before running the dag. The create statements
are in airflow/create_tables.sql. 
I did create a helper function that allows to these steps:
- redshift/create_redshift_cluster.py
- redshift/create_tables.py
- redshift/delete_redshift_cluster.py

You do need to create a dwh.cfg files with all credentials.

## How-to
- create redshift cluster (if it doesn't exist already you
can run redshift/create_redshift_cluster.py)
- create tables by running redshift/create_tables.py
- add 'aws_credentials' and 'redshift' connection in airflow
- activate dag
- delete cluster by running redshift/delete_redshift_cluster.py

## Schema

Using the song and event datasets, we create a star schema 
optimized for queries on song play analysis. This includes 
the following tables (all table definitions are in 
create_tables.sql)

### Staging tables
- staging_events
- staging_songs

### Dimension Tables

### Fact Tables

## Start Airflow Server
```
/opt/airflow/start.sh
```

## Dev Setup
```
virtualenv .env
source venv/bin/activate
python -m pip install -r requirements.txt
```
