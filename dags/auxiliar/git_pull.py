from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import today

with DAG(
        'executar_git_pull',
        start_date=today('America/Sao_Paulo'),
        schedule = None,
        tags = ['git', 'bash', 'aux'],
        catchup=False ) as dag:

    sync_repo = BashOperator(
        task_id='sync_repo',
        bash_command=f"""
            cd ~/airflow
            git pull
        """
    )

    sync_repo