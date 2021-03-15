import datetime
from airflow import DAG

from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator,
    PostgresOperator
)

from helpers import SqlQueries

dag = DAG(
    "udac_sparkify_dag",
    start_date=datetime.datetime.utcnow()
)

# dag = DAG('udac_sparkify_dag',
#           default_args=default_args,
#           description='Load and transform data in Redshift with Airflow',
#           schedule_interval='0 * * * *'
#         )

create_trips_table = PostgresOperator(
    task_id="create_staging_events",
    dag=dag,
    postgres_conn_id="redshift",
    sql=SqlQueries.create_staging_events
)
