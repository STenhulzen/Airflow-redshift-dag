from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.empty import EmptyOperator

from plugins.operators.stage_redshift import RedshiftStageOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries


# ------------------------------
# Default DAG arguments
# ------------------------------
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "retries": 3,
    "retry_delay": timedelta(minutes=5),
    "catchup": False,
}


# ------------------------------
# DAG definition
# ------------------------------
with DAG(
    dag_id="udac_redshift_etl",
    default_args=default_args,
    description="Stage data from S3 to Redshift and run ETL",
    schedule_interval="@hourly",
    max_active_runs=1,
) as dag:

    # ------------------------------
    # Start / End
    # ------------------------------
    start_execution = EmptyOperator(
        task_id="start_execution"
    )

    end_execution = EmptyOperator(
        task_id="end_execution"
    )

    # ------------------------------
    # Staging tasks
    # ------------------------------
    stage_events = RedshiftStageOperator(
        task_id="stage_events",
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="YOUR_S3_BUCKET",
        s3_key="log_data",
        json_path="s3://YOUR_S3_BUCKET/log_json_path.json",
        region="us-west-2",
        truncate_before_load=True,
    )

    stage_songs = RedshiftStageOperator(
        task_id="stage_songs",
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="YOUR_S3_BUCKET",
        s3_key="song_data",
        json_path="auto",
        region="us-west-2",
        truncate_before_load=True,
    )

    # ------------------------------
    # Fact table load
    # ------------------------------
    load_songplays_fact = LoadFactOperator(
        task_id="load_songplays_fact",
        table="songplays",
        redshift_conn_id="redshift",
        sql_insert=SqlQueries.songplay_table_insert,
    )

    # ------------------------------
    # Dimension table loads
    # ------------------------------
    load_user_dim = LoadDimensionOperator(
        task_id="load_user_dim",
        table="users",
        redshift_conn_id="redshift",
        sql_insert=SqlQueries.user_table_insert,
        mode="delete-load",
    )

    load_song_dim = LoadDimensionOperator(
        task_id="load_song_dim",
        table="songs",
        redshift_conn_id="redshift",
        sql_insert=SqlQueries.song_table_insert,
        mode="delete-load",
    )

    load_artist_dim = LoadDimensionOperator(
        task_id="load_artist_dim",
        table="artists",
        redshift_conn_id="redshift",
        sql_insert=SqlQueries.artist_table_insert,
        mode="delete-load",
    )

    load_time_dim = LoadDimensionOperator(
        task_id="load_time_dim",
        table="time",
        redshift_conn_id="redshift",
        sql_insert=SqlQueries.time_table_insert,
        mode="delete-load",
    )

    # ------------------------------
    # Data quality checks
    # ------------------------------
    run_quality_checks = DataQualityOperator(
        task_id="run_quality_checks",
        redshift_conn_id="redshift",
        tests=SqlQueries.quality_checks,
    )

    # ------------------------------
    # Task dependencies
    # ------------------------------
    start_execution >> [stage_events, stage_songs]
    [stage_events, stage_songs] >> load_songplays_fact
    load_songplays_fact >> [
        load_user_dim,
        load_song_dim,
        load_artist_dim,
        load_time_dim,
    ]
    [
        load_user_dim,
        load_song_dim,
        load_artist_dim,
        load_time_dim,
    ] >> run_quality_checks
    run_quality_checks >> end_execution
