#Instructions
#In this exercise, we’ll refactor a DAG with a single overloaded task into a DAG with several tasks with well-defined boundaries
#1 - Read through the DAG and identify points in the DAG that could be split apart
#2 - Split the DAG into multiple PythonOperators
#3 - Run the DAG

import datetime
import logging

from airflow import DAG
from airflow.hooks.postgres_hook import PostgresHook

from airflow.operators.postgres_operator import PostgresOperator
from airflow.operators.python_operator import PythonOperator

def youngest_rider():
    redshift_hook = PostgresHook("redshift")
    
    records = redshift_hook.get_records("""
        SELECT birthyear FROM younger_riders ORDER BY birthyear DESC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Youngest rider was born in {records[0][0]}")

#
# TODO: Finish refactoring this function into the appropriate set of tasks,
#       instead of keeping this one large task.
#
def how_often_each_bike_is_ridden():
    redshift_hook = PostgresHook("redshift")

    # Find out how often each bike is ridden
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS lifetime_rides;
        CREATE TABLE lifetime_rides AS (
            SELECT bikeid, COUNT(bikeid)
            FROM trips
            GROUP BY bikeid
        );
        COMMIT;
    """)

def number_of_stations_by_city():
    redshift_hook = PostgresHook("redshift")

    # Count the number of stations by city
    redshift_hook.run("""
        BEGIN;
        DROP TABLE IF EXISTS city_station_counts;
        CREATE TABLE city_station_counts AS(
            SELECT city, COUNT(city)
            FROM stations
            GROUP BY city
        );
        COMMIT;
    """)


def log_oldest():
    redshift_hook = PostgresHook("redshift")
    records = redshift_hook.get_records("""
        SELECT birthyear FROM older_riders ORDER BY birthyear ASC LIMIT 1
    """)
    if len(records) > 0 and len(records[0]) > 0:
        logging.info(f"Oldest rider was born in {records[0][0]}")


dag = DAG(
    "lesson3.exercise2",
    start_date=datetime.datetime.utcnow()
)

how_often_each_bike_is_ridden_task = PythonOperator(
    task_id='how_often_each_bike_is_ridden',
    dag=dag,
    python_callable=how_often_each_bike_is_ridden
)

number_of_stations_by_city_task = PythonOperator(
    task_id='number_of_stations_by_city',
    dag=dag,
    python_callable=number_of_stations_by_city
)

print_youngest_rider = PythonOperator(
    task_id='youngest_rider',
    python_callable=youngest_rider,
    dag=dag
)

create_oldest_task = PostgresOperator(
    task_id="create_oldest",
    dag=dag,
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS older_riders;
        CREATE TABLE older_riders AS (
            SELECT * FROM trips WHERE birthyear > 0 AND birthyear <= 1945
        );
        COMMIT;
    """,
    postgres_conn_id="redshift"
)

log_oldest_task = PythonOperator(
    task_id="log_oldest",
    dag=dag,
    python_callable=log_oldest
)

# Find all trips where the rider was under 18
create_younger_than_18_task = PostgresOperator(
    task_id='create_younger_than_18',
    dag=dag,
    postgres_conn_id='redshift',
    sql="""
        BEGIN;
        DROP TABLE IF EXISTS younger_riders;
        CREATE TABLE younger_riders AS (
            SELECT * FROM trips WHERE birthyear > 2000
        );
        COMMIT;
    """
)

how_often_each_bike_is_ridden_task
number_of_stations_by_city_task
create_oldest_task >> log_oldest_task
create_younger_than_18_task >> print_youngest_rider