import datetime
import os
from pathlib import Path

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.subdag_operator import SubDagOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

count_query = "SELECT COUNT(*) from {}"

def base_create_query():
    fileHandler = open(os.path.join(Path(__file__).parents[1], 'create_tables.sql'), "r")
    all_table_creates = " ".join(fileHandler.readlines())
    fileHandler.close()
    return f"""
    BEGIN;
    DROP TABLE IF EXISTS staging_events;
    DROP TABLE IF EXISTS staging_songs;
    DROP TABLE IF EXISTS songplays;
    DROP TABLE IF EXISTS users;
    DROP TABLE IF EXISTS songs;
    DROP TABLE IF EXISTS artists;
    DROP TABLE IF EXISTS time;
    {all_table_creates}
    COMMIT;
    """


start_date = datetime.datetime.utcnow()

default_args = {
    'owner': 'claudiordgz',
    'start_date': start_date,
    'email': ['claudio.rdgz@gmail.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'depends_on_past' : False,
    'retry_delay': datetime.timedelta(minutes=5)
    # catchup does not go here
}

dag = DAG('dend_project5',
          default_args=default_args,
          description='Load and transform Sparkify data in Redshift with Airflow',
          schedule_interval='0 * * * *',
          catchup=False
        )

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

drop_and_create_tables_task = PostgresOperator(
    task_id="drop_and_create_tables",
    dag=dag,
    sql=base_create_query(),
    postgres_conn_id='redshift'
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_events",
    s3_bucket="udacity-dend",
    s3_key="log_data"
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    table="staging_songs",
    s3_bucket="udacity-dend",
    s3_key="song_data"
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name='users'
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name='songs'
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name='artists'
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    dimension_name='time'
)

run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    tests=[
        {"test": count_query.format("songplays"), "expected_result": [6820] } , 
        {"test": count_query.format("songs"), "expected_result": [14896] },
        {"test": count_query.format("users"), "expected_result": [104] },
        {"test": count_query.format("artists"), "expected_result": [10025] },
        {"test": count_query.format("time"), "expected_result": [6813] }
    ]
)

end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

start_operator >> drop_and_create_tables_task

drop_and_create_tables_task >> stage_events_to_redshift
drop_and_create_tables_task >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table
stage_songs_to_redshift >> load_songplays_table

load_songplays_table >> load_song_dimension_table
load_songplays_table >> load_user_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table

load_song_dimension_table >> run_quality_checks
load_user_dimension_table >> run_quality_checks
load_artist_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks

run_quality_checks >> end_operator