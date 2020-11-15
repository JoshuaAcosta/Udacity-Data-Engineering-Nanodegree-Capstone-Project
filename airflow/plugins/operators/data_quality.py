"""Custom airflow operator to check data quality"""
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    Takes sql query to check data quality
    recently copied to Redshift.
    """
    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 data_quality_check="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.check = data_quality_check
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        redshift_hook = PostgresHook("redshift_conn")
        exp_result = 0
        records = redshift_hook.get_records(self.check)
        num_records = records[0][0]
        if num_records != exp_result:
            raise ValueError("Data quality check failed.")
        else:
            self.log.info("Data quality passed")
