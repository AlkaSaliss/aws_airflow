from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 list_tests=[],
                 *args, **kwargs):
        """Operator that checks data quality after ETL.
            Currently only doing a verification that tables are not empty 

        Args:
            redshift_conn_id (str, optional): Name of the redshift (postgres) connection. Defaults to "".
            list_tests (list, optional): list of dictionaries, each dict has two keys,
                `test` indicating the test query to run and `result` corresponding to the expected  results. Defaults to [].
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_tests = list_tests
    
    def _log_formatted(self, msg:str):
        self.log.info("+-"*100)
        self.log.info("\t"+str(msg))
        self.log.info("+-"*100+"\n")
    
    def execute(self, context):
        self._log_formatted("Settings connections")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        list_failed = []
        for test_case in self.list_tests:
            query = test_case["test"]
            records = redshift.get_records(query)
            if records is None or len(records[0]) < test_case["result"]:
                list_failed.append(test_case)
            else:
                self._log_formatted(f"Test : {query} succeeded")
        if (len(list_failed)) > 0:
            for query in list_failed:
                self._log_formatted(f"Test : {query} Failed")
            raise ValueError(f"{len(list_failed)} Data quality checks failed")
