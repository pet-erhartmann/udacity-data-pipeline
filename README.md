# udacity-data-pipeline

## Requirements
- tables need to be created before running the dag (from create_tables.sql)

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
