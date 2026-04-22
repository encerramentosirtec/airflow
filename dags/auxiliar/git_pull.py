from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import today, timezone
from datetime import datetime

with DAG(
        'git_pull',
        start_date=datetime(2026, 4, 16, tzinfo=timezone("America/Sao_Paulo")),
        schedule = None,
        tags = ['git', 'bash', 'aux'],
        catchup=False ) as dag:

    sync_repo = BashOperator(
        task_id='sync_repo',
        bash_command=f"""
            cd ~/airflow
            git pull
            source airenv/bin/activate
            export AIRFLOW_HOME=~/airflow
            airflow dags reserialize
        """
    )

    sync_repo