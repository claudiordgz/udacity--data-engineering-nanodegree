from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

import json 

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 dimension_name="",
                 tests=None,
                 *args, **kwargs):
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.dimension_name = dimension_name
        self.tests = tests

    def execute(self, context):
        self.log.info("DataQualityOperator: Starting execution")

        if not self.tests:
            raise AttributeError("DataQualityOperator: Test Dict[str,str] is required.")
        
        pg_hook = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for test in self.tests:
            sql_statement = test["test"]
            expected = test["expected_result"]
            self.log.info(f"DataQualityOperator: Processing Test")
            records = pg_hook.get_records(sql_statement)
            result = records[0]
            result_json = json.dumps(result)
            expected_json = json.dumps(expected)
            self.log.info(f"Result: {result_json}")
            self.log.info(f"Expected: {expected_json}")
            if result_json != expected_json:
                self.log.info("Incorrect test result")
                raise ValueError("Fail: Test results are incorrect")
            else:
                self.log.info("Passed")

        self.log.info(f":DataQualityOperator: Finished Execution")