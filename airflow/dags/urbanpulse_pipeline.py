# airflow/dags/urbanpulse_pipeline.py

from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator


default_args = {
    "owner": "urbanpulse"
}


with DAG(
    dag_id="urbanpulse_pipeline",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
    default_args=default_args
) as dag:

    bronze_to_weather = BashOperator(
        task_id="bronze_to_weather",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.bronze_to_silver_weather
        """
    )

    bronze_to_air = BashOperator(
        task_id="bronze_to_air",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.bronze_to_silver_air_quality
        """
    )

    bronze_to_traffic = BashOperator(
        task_id="bronze_to_traffic",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.bronze_to_silver_traffic
        """
    )

    gold_metrics = BashOperator(
        task_id="gold_metrics",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.gold_city_metrics
        """
    )

    stress_index = BashOperator(
        task_id="stress_index",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.stress_index_engine
        """
    )

    alerts = BashOperator(
        task_id="alerts",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.alert_engine
        """
    )

    (
        [
            bronze_to_weather,
            bronze_to_air,
            bronze_to_traffic
        ]
        >>
        gold_metrics
        >>
        stress_index
        >>
        alerts
    )



