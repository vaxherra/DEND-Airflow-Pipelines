import os
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (StageToRedshiftOperator, LoadFactOperator,
                                LoadDimensionOperator, DataQualityOperator)
from airflow.operators.postgres_operator import PostgresOperator
from airflow.contrib.hooks.aws_hook import AwsHook
from helpers import SqlQueries

#############################################
##### DEFINE DEFAULT ARGUMENTS FOR ETL ######
#############################################

default_args = {
    'owner': 'Robert Kwapich',
    'start_date': datetime(2020, 3, 21),
    'end_date': datetime(2020, 3, 22),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False
}

dag = DAG('sparkify_tests',
          default_args=default_args,
          description='Load and transform data in Redshift with Airflow',
          schedule_interval="@hourly", # alternatively '0 * * * *' # once an hour Crone code
          max_active_runs=1
        )

#############################################
######### DEFINE A SET OF ETL TASKS #########
#############################################

start_operator = DummyOperator(task_id='Begin_execution',  dag=dag)

create_tables = PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='/create_tables.sql'  
)

stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    
    table="staging_events",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="log_data",
    JSONPaths="log_json_path.json"

)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    
    table="staging_songs",
    redshift_conn_id="redshift",
    aws_credentials_id="aws_credentials",
    s3_bucket="udacity-dend",
    s3_key="song_data",
    JSONPaths="auto"
    
)
 
load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id="redshift",
    target_table="songplays",
    target_columns="playid,start_time,userid,level,songid,artistid,sessionid,location,user_agent",
    insert_mode="append", # delete_load/append
    query=SqlQueries.songplay_table_insert
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    
    redshift_conn_id="redshift",
    target_table="users",
    target_columns="userid,first_name,last_name,gender,level",
    insert_mode="append", # delete_load/append
    query=SqlQueries.user_table_insert
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    
    redshift_conn_id="redshift",
    target_table="songs",
    target_columns="songid, title, artistid, year, duration",
    insert_mode="append", # delete_load/append
    query=SqlQueries.song_table_insert
    
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    
    redshift_conn_id="redshift",
    target_table="artists",
    target_columns="artistid, name, location, lattitude,longitude ",
    insert_mode="append", # delete_load/append
    query=SqlQueries.artist_table_insert
    
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    
    redshift_conn_id="redshift",
    target_table="time",
    target_columns="start_time, hour, day, week, month, year, dayofweek ",
    insert_mode="append", # delete_load/append
    query=SqlQueries.time_table_insert
   
)
 
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id="redshift",
    tests=[ (SqlQueries.songplays_check_nulls, "{}[0][0] == 0" ),
           
           (SqlQueries.users_check_nulls, "{}[0][0] == 0" ),
           (SqlQueries.songs_check_nulls, "{}[0][0] == 0" ),
           (SqlQueries.artists_check_nulls, "{}[0][0] == 0" ),
           (SqlQueries.time_check_nulls, "{}[0][0] == 0" ),
          ]
)


end_operator = DummyOperator(task_id='Stop_execution',  dag=dag)

#############################################
##### BUILDING A DAG ORDER DEPENDENCIES #####
#############################################
start_operator >> create_tables

create_tables >> stage_events_to_redshift
create_tables >> stage_songs_to_redshift

stage_events_to_redshift >> load_songplays_table 
stage_songs_to_redshift >> load_songplays_table


load_songplays_table >> load_user_dimension_table
load_songplays_table>> load_song_dimension_table
load_songplays_table >> load_artist_dimension_table
load_songplays_table >> load_time_dimension_table 

load_user_dimension_table >> run_quality_checks
load_song_dimension_table >> run_quality_checks
load_time_dimension_table >> run_quality_checks
load_artist_dimension_table >>run_quality_checks

run_quality_checks >> end_operator