from airflow import DAG
from airflow.operators import (LoadDimensionOperator, DataQualityOperator)

def load_dim_tables_dag(
        parent_dag_name,
        task_id,
        redshift_conn_id,
        table,
        sql,
        columns,
        truncate_table=False,
        *args, **kwargs):
    dag = DAG(
        f"{parent_dag_name}.{task_id}",
        **kwargs
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id=f"load_{table}_dim_table",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        sql=sql,
        truncate_table=truncate_table
    )

    run_quality_checks = DataQualityOperator(
        task_id=f"run_{table}_quality_checks",
        dag=dag,
        redshift_conn_id=redshift_conn_id,
        table=table,
        columns=columns
    )

    load_user_dimension_table >> run_quality_checks

    return dag
