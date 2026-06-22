from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

with DAG(
    dag_id="bronze_to_silver",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    weather = BashOperator(
        task_id="weather_silver",
        bash_command="""
        cd ~/urbanpulse &&
        source venv/bin/activate &&
        python -m spark.bronze_to_silver_weather
        """
    )

    air = BashOperator(
        task_id="air_silver",
        bash_command="""
        cd ~/urbanpulse &&
        source venv/bin/activate &&
        python -m spark.bronze_to_silver_air_quality
        """
    )

    traffic = BashOperator(
        task_id="traffic_silver",
        bash_command="""
        cd ~/urbanpulse &&
        source venv/bin/activate &&
        python -m spark.bronze_to_silver_traffic
        """
    )

    [weather, air, traffic]


