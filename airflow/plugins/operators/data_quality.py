from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 columns=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.columns = columns

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        # CHECK FOR NO ROWS
        records_in_table_query = f"SELECT COUNT(*) FROM {self.table}"
        records_in_table = redshift.get_records(records_in_table_query)

        print(records_in_table_query)
        print(records_in_table[0][0])

        if records_in_table[0][0] < 1:
            raise ValueError(f"Data quality check failed. {self.table} returned no results")

        # CHECK FOR NULL RECORDS
        for c in self.columns:
            null_values_query = f"SELECT COUNT(*) FROM {self.table} WHERE {c} IS NULL"
            null_values = redshift.get_records(null_values_query)

            print(null_values_query)
            print(null_values[0][0])

            if null_values[0][0] > 0:
                raise ValueError(f"Data quality check failed. column {c} in table {self.table} returned null values")
