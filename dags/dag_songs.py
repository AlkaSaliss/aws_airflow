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
    "start_date": datetime(2020, 12, 21),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "email_on_retry": False
}

dag = DAG('songs_data_dag',
          default_args=default_args,
          description='Load and transform songs data in Redshift with Airflow',
          schedule_interval=None,
          catchup=False
        )

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
stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
	redshift_conn_id="redshift",
	aws_credentials_id="aws_credentials",
	table="staging_songs",
	s3_bucket="udacity-dend",
	s3_key="song_data",
	region="us-west-2",
	json_format="auto",
	time_format="auto"
)

# songs dimension table insert
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

# Task ordering
create_staging_songs >> stage_songs_to_redshift
stage_songs_to_redshift >> load_songs
create_songs >> load_songs
stage_songs_to_redshift >> load_artists
create_artists >> load_artists