from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        records = redshift.get_records(f"SELECT COUNT(*) FROM {self.table}")
        print(f"SELECT COUNT(*) FROM {self.table}")
        print(records[0][0])
        if records[0][0] < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")
