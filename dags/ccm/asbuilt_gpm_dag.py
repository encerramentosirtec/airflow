#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from pendulum import duration, today, timezone
from datetime import datetime
from src.bots_ccm import Bots

bot = Bots(cred_file='global_brook.json')

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 2,
    'owner' : 'bob'
}

with DAG('asbuilt',
        default_args = default_args,
        #default_view="graph",
        start_date=datetime(2026, 4, 16, tzinfo=timezone("America/Sao_Paulo")),
        schedule = '29,59 7-20 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False,
        on_failure_callback=[
            send_smtp_notification(
                from_email="sirtec.heli@gmail.com",
                to="heli.silva@sirtec.com.br",
                subject="[Airflow] The dag {{ dag.dag_id }} failed",
                html_content="debug logs",
            )
        ],
        ) as dag:
    
    consulta_asbuilt_gpm = PythonOperator(
        task_id='consulta_asbuilt_gpm',
        python_callable=bot.consulta_asbuilt_gpm,
        retries=2,
        retry_delay=duration(seconds=20),
        execution_timeout=duration(minutes=10)
    )

    processa_asbuilt_gpm = PythonOperator(
        task_id='processa_asbuilt_gpm',
        python_callable=bot.processa_asbuilt_gpm,
        retries=2,
        retry_delay=duration(seconds=20),
        execution_timeout=duration(minutes=10)
    )

    consulta_asbuilt_gpm >> processa_asbuilt_gpm