from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'


    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 insert_truncate=False,
                 query="",
                 *args, **kwargs):
        """Airflow operator for loading dimension table from staging tables in aws Redshift

        Args:
            redshift_conn_id (str, optional): Name of the redshift (postgres) connection. Defaults to "".
            table (str, optional): name of the dimension table. Defaults to "".
            insert_truncate (bool, optional): whether to delete table content before inserting new data. Defaults to False.
            query (str, optional): sql query defining the insert statement. Defaults to "".
        """

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.insert_truncate = insert_truncate
        self.query = query
    
    def _log_formatted(self, msg:str):
        self.log.info("+-"*100)
        self.log.info("\t"+str(msg))
        self.log.info("+-"*100+"\n")
    
    def execute(self, context):
        self._log_formatted("Settings connections")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        if self.insert_truncate:
            self._log_formatted(f"Clearing data from table : {self.table}")
            redshift.run(f"DELETE FROM {self.table}")

        self._log_formatted(f"Loading data to table {self.table}")
        redshift.run(self.query)