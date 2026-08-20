#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import duration, today, timezone
from datetime import datetime
from src.bots_ccm import Bots

bot = Bots(cred_file='jimmy.json')

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 2,
    'owner' : 'bob'
}

with DAG('pastas',
        default_args = default_args,
        #default_view="graph",
        start_date=datetime(2026, 4, 16, tzinfo=timezone("America/Sao_Paulo")),
        schedule = '30 7-20/1 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex', 'pastas'],
        catchup = False) as dag:
    
    pastas = PythonOperator(
        task_id='pastas',
        python_callable=bot.atualiza_pasta,
        retries=2,
        retry_delay=duration(seconds=20),
        execution_timeout=duration(hours=2)
    )

    pastas