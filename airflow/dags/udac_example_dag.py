from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator,
                              PostgresOperator)
from airflow.operators.subdag_operator import SubDagOperator
from udac_subdag import load_dim_tables_dag

from helpers import SqlQueries

# AWS_KEY = os.environ.get('AWS_KEY')
# AWS_SECRET = os.environ.get('AWS_SECRET')

default_args = {
    'owner': 'udacity',
    #'start_date': datetime(2019, 1, 12),
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False
}

start_date=datetime(2019, 1, 12)

dag = DAG('udac_example_dag',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          start_date=start_date,
          max_active_runs=1
        )

start_operator = DummyOperator(
    task_id='Begin_execution',
    dag=dag
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log-data/2018/11/2018-11-02-events.json",
    json="s3://udacity-dend/log_json_path.json"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song-data/A/A/A"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    table="songplays",
    sql=SqlQueries.songplay_table_insert
)

users_task_id = "users_dim_subdag"
users_subdag_task = SubDagOperator(
    subdag=load_dim_tables_dag(
        "udac_example_dag",
        users_task_id,
        redshift_conn_id="redshift",
        table="users",
        sql=SqlQueries.user_table_insert,
        columns=["userid"],
        start_date=start_date,
    ),
    task_id=users_task_id,
    dag=dag,
)

songs_task_id = "songs_dim_subdag"
songs_subdag_task = SubDagOperator(
    subdag=load_dim_tables_dag(
        "udac_example_dag",
        songs_task_id,
        redshift_conn_id="redshift",
        table="songs",
        sql=SqlQueries.song_table_insert,
        columns=["songid"],
        start_date=start_date,
    ),
    task_id=songs_task_id,
    dag=dag,
)

artists_task_id = "artists_dim_subdag"
artists_subdag_task = SubDagOperator(
    subdag=load_dim_tables_dag(
        "udac_example_dag",
        artists_task_id,
        redshift_conn_id="redshift",
        table="artists",
        sql=SqlQueries.artist_table_insert,
        columns=["artistid"],
        start_date=start_date,
    ),
    task_id=artists_task_id,
    dag=dag,
)

time_task_id = "time_dim_subdag"
time_subdag_task = SubDagOperator(
    subdag=load_dim_tables_dag(
        "udac_example_dag",
        time_task_id,
        redshift_conn_id="redshift",
        table="time",
        sql=SqlQueries.time_table_insert,
        columns=["start_time"],
        start_date=start_date,
    ),
    task_id=time_task_id,
    dag=dag,
)

end_operator = DummyOperator(
    task_id='End_execution',
    dag=dag
)

start_operator >> stage_events_to_redshift
start_operator >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> users_subdag_task
load_songplays_table >> songs_subdag_task
load_songplays_table >> artists_subdag_task
load_songplays_table >> time_subdag_task

users_subdag_task >> end_operator
songs_subdag_task >> end_operator
artists_subdag_task >> end_operator
time_subdag_task >> end_operator
