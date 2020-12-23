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

dag = DAG('sparkify_etl_dag',
          default_args=default_args,
          description='Load and transform events and songs data in Redshift with Airflow',
          schedule_interval="@hourly",
		  max_active_runs=1,
        )


# dummy start and end tasks
start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)
end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

# Table creation tasks
create_staging_events = PostgresOperator(
	task_id="create_staging_events",
	postgres_conn_id="redshift",
	dag=dag,
	sql=SqlQueries.staging_events_table_create
)
create_staging_songs = PostgresOperator(
	task_id="create_staging_songs",
	postgres_conn_id="redshift",
	dag=dag,
	sql=SqlQueries.staging_songs_table_create
)
create_songs = PostgresOperator(
	task_id="create_songs",
	postgres_conn_id="redshift",
	dag=dag,
	sql=SqlQueries.song_table_create
)
create_artists = PostgresOperator(
	task_id="create_artists",
	postgres_conn_id="redshift",
	dag=dag,
	sql=SqlQueries.artist_table_create
)
create_users = PostgresOperator(
	task_id="create_users",
	postgres_conn_id="redshift",
	dag=dag,
	sql=SqlQueries.user_table_create
)
create_time = PostgresOperator(
	task_id="create_time",
	postgres_conn_id="redshift",
	dag=dag,
	sql=SqlQueries.time_table_create
)
create_songplays = PostgresOperator(
	task_id="create_songplays",
	dag=dag,
	postgres_conn_id="redshift",
	sql=SqlQueries.songplay_table_create
)

# songs data staging task
bucket_name = "udacity-dend"
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
	redshift_conn_id="redshift",
	aws_credentials_id="aws_credentials",
	table="staging_songs",
	s3_bucket=bucket_name,
	s3_key="song_data",
	region="us-west-2",
	json_format="auto",
	time_format="auto"
)

# events data staging task
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
	redshift_conn_id="redshift",
	aws_credentials_id="aws_credentials",
	table="staging_events",
	s3_bucket=bucket_name,
	s3_key="log_data/{execution_date.year}/{execution_date.month}/{ds}-events.json",
	region="us-west-2",
	json_format="s3://udacity-dend/log_json_path.json",
	time_format="epochmillisecs"
)

# songs dimension tables insert
load_songs = LoadDimensionOperator(
	task_id="load_songs",
	dag=dag,
	redshift_conn_id="redshift",
	table="songs",
	insert_truncate=False,
	query=SqlQueries.song_table_insert
)

# artists dimension table insert
load_artists = LoadDimensionOperator(
	task_id="load_artists",
	dag=dag,
	redshift_conn_id="redshift",
	table="artists",
	insert_truncate=False,
	query=SqlQueries.artist_table_insert
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
# data quality operator
list_tests = []
for tab_ in ["songplays", "songs", "artists", "users", "time"]:
	list_tests.append({
		'test': f"SELECT COUNT(*) FROM {tab_}" , 'result': 1 # check if nb records >= 1
	})

data_quality_check = DataQualityOperator(
	task_id="data_quality_check",
	dag=dag,
	redshift_conn_id="redshift",
	list_tests=list_tests,
)

# Task ordering
# create tables
start_operator >> create_staging_songs 
start_operator >> create_staging_events
start_operator >> create_time
start_operator >> create_users
start_operator >> create_artists
create_artists >> create_songs
create_songs >> create_songplays
create_artists >> create_songplays
create_users >> create_songplays
create_time >> create_songplays

# staging
create_songplays >> stage_events_to_redshift
create_songplays >> stage_songs_to_redshift
create_staging_songs >> stage_events_to_redshift
create_staging_songs >> stage_songs_to_redshift
create_staging_events >> stage_events_to_redshift
create_staging_events >> stage_songs_to_redshift
# fact table
stage_events_to_redshift >> load_songplays
stage_songs_to_redshift >> load_songplays
# dimension tables
load_songplays >> load_songs
load_songplays >> load_users
load_songplays >> load_artists
load_songplays >> load_time
# quality check
load_songs >> data_quality_check
load_users >> data_quality_check
load_artists >> data_quality_check
load_time >> data_quality_check
# end execution
data_quality_check >> end_operator
data_quality_check >> end_operator
data_quality_check >> end_operator
data_quality_check >> end_operator
