from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
from growth import growth_detection
from metrics import metrics_collector
from multi_modal.preprocess import preprocess_keywords

# Variable to use spark
USE_SPARK = os.getenv("USE_SPARK", "false").lower() == "true"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "tiktok_multimodal_pipeline",
    default_args=default_args,
    description="TikTok trending keyword extraction pipeline",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
) as dag:

    # Step 1: run scraper inside its own container
    scrape = DockerOperator(
        task_id="scrape_tiktok",
        image="scraper:latest",
        api_version="auto",
        auto_remove=True,
        command="python scraper1.py",
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 2: run multimodal pipeline inside its own container
    multimodal_command = ("python multimodal_spark.py" if USE_SPARK else "python multimodal_pipeline.py")

    multimodal = DockerOperator(
        task_id="multimodal_pipeline",
        image="multimodal:latest",
        api_version="auto",
        auto_remove=True,
        command=multimodal_command,
        docker_url="unix://var/run/docker.sock",
        network_mode="bridge",
    )

    # Step 3: Creating clean keywords csv (for API and dashboard)
    preprocess_task = PythonOperator(
        task_id="preprocess_keywords",
        python_callable=preprocess_keywords,
        op_kwargs={
            "input_path": "/opt/airflow/data/keywords.csv",
            "output_path": "/opt/airflow/data/keywords_clean.csv",
        },
    )
    
    # Step 4: refresh TimescaleDB rollups
    refresh_rollups = PostgresOperator(
        task_id="refresh_rollups",
        postgres_conn_id="tiktok_db",
        sql="REFRESH MATERIALIZED VIEW CONCURRENTLY keyword_hourly_counts;",
    )

    # Step 5: growth detection
    growth_task = PythonOperator(
        task_id="compute_keyword_growth",
        python_callable=growth_detection.detect_trending_keywords,
        op_kwargs={
            "time_window": "hourly",
            "growth_threshold": 0.5,
            "region": None,
            "category": None,
            "min_likes": 50,
            "min_comments": 5,
            "min_shares": 2,
            "min_views": 1000,
            "limit": 20,
        },
    )

    # Step 6: Collect pipeline metrics
    collect_metrics = PythonOperator(
        task_id="collect_pipeline_metrics",
        python_callable=metrics_collector.collect_pipeline_metrics,
    )

    scrape >> multimodal >> preprocess_task >> refresh_rollups >> growth_task >> collect_metrics
