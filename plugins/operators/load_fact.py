from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 table="",
                 query="",
                 *args, **kwargs):
        """Airflow operator for loading fact table from staging tables in aws Redshift

        Args:
            redshift_conn_id (str, optional): Name of the redshift (postgres) connection. Defaults to "".
            table (str, optional): name of the dimension table. Defaults to "".
            query (str, optional): sql query defining the insert statement. Defaults to "".
        """
        super(LoadFactOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.query = query
    
    def _log_formatted(self, msg:str):
        self.log.info("+-"*100)
        self.log.info("\t"+str(msg))
        self.log.info("+-"*100+"\n")
    
    def execute(self, context):
        self._log_formatted("Settings connections")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        self._log_formatted(f"Loading data to table {self.table}")
        redshift.run(self.query)