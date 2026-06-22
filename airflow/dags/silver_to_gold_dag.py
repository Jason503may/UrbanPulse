from datetime import datetime

from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="silver_to_gold",
    start_date=datetime(2025, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    gold = BashOperator(
        task_id="gold_metrics",
        bash_command="""
        cd /opt/urbanpulse &&
        python -m spark.gold_city_metrics
        """
    )

    stress = BashOperator(
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

    gold >> stress >> alerts

