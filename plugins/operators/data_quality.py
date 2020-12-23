from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 list_tables=[],
                 *args, **kwargs):
        """Operator that checks data quality after ETL.
            Currently only doing a verification that tables are not empty 

        Args:
            redshift_conn_id (str, optional): Name of the redshift (postgres) connection. Defaults to "".
            list_tables (list, optional): list of table names on which to apply quality checks. Defaults to [].
        """
        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.list_tables = list_tables
    
    def _log_formatted(self, msg:str):
        self.log.info("+-"*100)
        self.log.info("\t"+str(msg))
        self.log.info("+-"*100+"\n")
    
    @staticmethod
    def table_not_empty(table, hook, logger):
        """Check if table is empty and raise exception

        Args:
            table (str): a table name on which to apply quality check
            hook (PostgresHook): postgres airflow hook
            logger (logger): logger for logging purpose

        Raises:
            ValueError: if table is empty
        """
        records = hook.get_records(f"SELECT COUNT(*) FROM {table}")
        logger.info("+-"*100)
        if records is None or len(records[0]) < 1:
            raise ValueError(f"NON-EMPTY Data quality check failed for table {table}\n"+"+-"*100+"\n")
        logger.info("\t"+f"NON-EMPTY Data quality check succeeded for table {table}\n"+"+-"*100+"\n")

    def execute(self, context):
        self._log_formatted("Settings connections")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for table in self.list_tables:
            DataQualityOperator.table_not_empty(table, redshift, self.log)
