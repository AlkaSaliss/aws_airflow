from airflow.contrib.hooks.aws_hook import AwsHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    copy_query = """
            COPY {} FROM '{}'
            ACCESS_KEY_ID '{}'
            SECRET_ACCESS_KEY '{}'
            REGION '{}'
            JSON '{}'
            TIMEFORMAT '{}';
    """
    
    @apply_defaults
    def __init__(self,
                 redshift_conn_id="",
                 aws_credentials_id="",
                 table="",
                 s3_bucket="",
                 s3_key="",
                 region="",
                 json_format="",
                 time_format="",
                 *args, **kwargs):
        """Airflow operator for Copying data from S3 to a Redshift staging table

        Args:
            redshift_conn_id (str, optional): Name of the redshift (postgres) connection. Defaults to "".
            aws_credentials_id (str, optional): Name of the aws connection. Defaults to "".
            table (str, optional): name of the staging table. Defaults to "".
            s3_bucket (str, optional): Name of the s3 bucket containing json files. Defaults to "".
            s3_key (str, optional): [templated] path to json files in s3. Defaults to "", variables from context will be used to fill the template if needed.
            region (str, optional): aws region. Defaults to "".
            json_format (str, optional): keyword (e.g. auto) or path to a json file describing the json files format. Defaults to "".
            time_format (str, optional): time format. Defaults to "".
        """

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key   
        self.region = region
        self.json_format = json_format
        self.time_format= time_format
    
    def _log_formatted(self, msg:str):
        self.log.info("+-"*100)
        self.log.info("\t"+str(msg))
        self.log.info("+-"*100+"\n")
    
    def execute(self, context):
        self._log_formatted("Settings connections")
        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        
        self._log_formatted(f"Clearing data from staging table : {self.table}")
        redshift.run(f"DELETE FROM {self.table}")

        rendered_key = self.s3_key.format(**context)
        s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
        formatted_query = StageToRedshiftOperator.copy_query.format(
            self.table,
            s3_path,
            credentials.access_key,
            credentials.secret_key,
            self.region,
            self.json_format,
            self.time_format
        )
        self._log_formatted(f"Copying data from {s3_path} to table {self.table}")
        redshift.run(formatted_query)
