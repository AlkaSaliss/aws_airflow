from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries


default_args = {
    "owner": "udacity",
    "start_date": datetime(2018, 11, 1),
	"end_date": datetime(2018, 11, 30),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False,
	"catchup": False
}

dag = DAG('events_data_dag',
          default_args=default_args,
          description='Load and transform events data in Redshift with Airflow',
          schedule_interval="@daily",
		  max_active_runs=1,
        )

# events data staging task
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
	redshift_conn_id="redshift",
	aws_credentials_id="aws_credentials",
	table="staging_events",
	s3_bucket="udacity-dend",
	s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
	region="us-west-2",
	json_format="s3://udacity-dend/log_json_path.json",
	time_format="epochmillisecs"
)

# songplays fact table insert 
load_songplays = LoadFactOperator(
	task_id="load_songplays",
	dag=dag,
	redshift_conn_id="redshift",
	table="songplays",
	query=SqlQueries.songplay_table_insert
)

# time dimension table insert
load_time = LoadDimensionOperator(
	task_id="load_time",
	dag=dag,
	redshift_conn_id="redshift",
	table="time",
	insert_truncate=False,
	query=SqlQueries.time_table_insert
) 

# users dimension table insert
load_users = LoadDimensionOperator(
	task_id="load_users",
	dag=dag,
	redshift_conn_id="redshift",
	table="users",
	insert_truncate=False,
	query=SqlQueries.user_table_insert
)

data_quality_check = DataQualityOperator(
	task_id="data_quality_check",
	dag=dag,
	redshift_conn_id="redshift",
	list_tables=["songplays", "songs", "artists", "users", "time"],
)

# tasks ordering
stage_events_to_redshift >> load_songplays
load_songplays >> load_time
stage_events_to_redshift >> load_users
load_time >> data_quality_check
load_users >> data_quality_check