from datetime import datetime, timedelta
from time import strftime
import os
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import (
    StageToRedshiftOperator,
    LoadFactOperator,
    LoadDimensionOperator,
    DataQualityOperator)
from ..plugins.helpers import LoadOperatorMode, SaveMode
from ..plugins.helpers import (
    TableIsNotEmptyDataQualityCheck,
    ColumnDoesNotContainNullsDataQualityCheck,
    CustomDataQualityCheck)

# DAG default parameters
default_args = {
    'owner': 'sparkify',
    'start_date': datetime(2020, 3, 21, 0, 0, 0),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False,
    'catchup': False,
}

# DAG declaration
dag = DAG(
    dag_id='Sparkify_song_events_dag',
    description='Load song and event data from S3, transform in Redshift and apply quality checks with Airflow.',
    default_args=default_args,
    schedule_interval='@hourly',
    max_active_runs=1
)

# DAG tasks
start_operator = DummyOperator(task_id='Begin_execution', dag=dag)

# `s3_key` supports template string and could be filled like this:
#
#   s3_key='log_data/{{ execution_date.strftime("%Y-%m-%d") }}-events.json'
#
# to load a single file from logs for a particular day.
# But for the sake of the demo in case we have limited amount of test data
# we will load all entire dataset.
stage_events_to_redshift = StageToRedshiftOperator(
    task_id='Stage_events',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws',
    aws_region='us-west-2',
    table='staging_events',
    s3_bucket='udacity-dend',
    s3_key='log_data/*'
)

stage_songs_to_redshift = StageToRedshiftOperator(
    task_id='Stage_songs',
    dag=dag,
    redshift_conn_id='redshift',
    aws_credentials_id='aws',
    aws_region='us-west-2',
    table='staging_songs',
    s3_bucket='udacity-dend',
    s3_key='song_data/*/*/*/*'
)

load_songplays_table = LoadFactOperator(
    task_id='Load_songplays_fact_table',
    dag=dag,
    redshift_conn_id='redshift',
    operator_mode=LoadOperatorMode.Table,
    table='songplays'
)

load_user_dimension_table = LoadDimensionOperator(
    task_id='Load_user_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    operator_mode=LoadOperatorMode.Table,
    table='users',
    save_mode=SaveMode.Overwrite
)

load_song_dimension_table = LoadDimensionOperator(
    task_id='Load_song_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    operator_mode=LoadOperatorMode.Table,
    table='songs',
    save_mode=SaveMode.Overwrite
)

load_artist_dimension_table = LoadDimensionOperator(
    task_id='Load_artist_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    operator_mode=LoadOperatorMode.Table,
    table='artists',
    save_mode=SaveMode.Overwrite
)

load_time_dimension_table = LoadDimensionOperator(
    task_id='Load_time_dim_table',
    dag=dag,
    redshift_conn_id='redshift',
    operator_mode=LoadOperatorMode.Table,
    table='time',
    save_mode=SaveMode.Overwrite
)

# Data quality checks is passed as array of instances of predefined classes (see DataQualityOperator docstring).
# Here is used two types of checks: TableIsNotEmptyDataQualityCheck which checks that table is not empty
# and ColumnDoesNotContainNullsDataQualityCheck to check if specific column doesn't contain NULLs.
# Also you can specify your own data quality check by passing an instance of the class CustomDataQualityCheck
# where you can pass parameters: `query` and `expected_result`.
run_quality_checks = DataQualityOperator(
    task_id='Run_data_quality_checks',
    dag=dag,
    redshift_conn_id='redshift',
    data_quality_checks=[
        TableIsNotEmptyDataQualityCheck('songplays'),
        TableIsNotEmptyDataQualityCheck('users'),
        TableIsNotEmptyDataQualityCheck('songs'),
        TableIsNotEmptyDataQualityCheck('artists'),
        TableIsNotEmptyDataQualityCheck('time'),

        ColumnDoesNotContainNullsDataQualityCheck('users', column_name='user_id'),
        ColumnDoesNotContainNullsDataQualityCheck('songs', column_name='song_id'),
        ColumnDoesNotContainNullsDataQualityCheck('artists', column_name='artist_id'),
    ]
)

end_operator = DummyOperator(task_id='Stop_execution', dag=dag)

# DAG flow declaration (task dependencies)
(
    start_operator
    >> [stage_events_to_redshift, stage_songs_to_redshift]
    >> load_songplays_table
    >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table]
    >> run_quality_checks
    >> end_operator
)
