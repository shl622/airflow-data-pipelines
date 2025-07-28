from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id="",
                 dq_checks=[],
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dq_checks = dq_checks

    def execute(self, context):
        redshift_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        for check in self.dq_checks:
            sql = check.get('check_sql')
            exp_result = check.get('expected_result')
            
            self.log.info(f"Running data quality check: {sql}")
            records = redshift_hook.get_records(sql)
            
            if len(records) < 1 or len(records[0]) < 1:
                raise ValueError(f"Data quality check failed. {sql} returned no results")
            
            num_records = records[0][0]
            if num_records != exp_result:
                raise ValueError(f"Data quality check failed. Expected: {exp_result}, Got: {num_records}")
            
            self.log.info(f"Data quality check passed: {sql}")
        
        self.log.info("All data quality checks passed")