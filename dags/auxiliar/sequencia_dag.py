#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from src.bots_auxiliar import Bots_aux
from pendulum import today, duration, timezone
from datetime import datetime

bot = Bots_aux()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('sequencia-de-pendencias',
        default_args = default_args,
        #default_view="graph",
        start_date=datetime(2026, 4, 16, tzinfo=timezone("America/Sao_Paulo")),
        schedule = '0,15,30,45 7,18 * * 1-6',
        max_active_runs = 1,
        tags = ['sequencia', 'gpm', 'aux'],
        catchup = False) as dag:

    sequencia = PythonOperator(
        task_id = 'sequencia',
        python_callable = bot.atualiza_sequencia
    )

    sequencia
    