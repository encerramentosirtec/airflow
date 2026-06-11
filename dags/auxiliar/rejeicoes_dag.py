#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from src.bots_auxiliar import Bots_aux
from pendulum import today, duration, timezone
from datetime import datetime

email = Bots_aux()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('rejeicoes',
        default_args = default_args,
        #default_view="graph",
        start_date=datetime(2026, 4, 16, tzinfo=timezone("America/Sao_Paulo")),
        schedule = None,#'0 8,11,14,17 * * 1-6',
        max_active_runs = 1,
        tags = ['rejeicoes', 'geoex', 'email', 'aux'],
        catchup = False) as dag:

    relatorio = PythonOperator(
        task_id='relatorio',
        python_callable=email.relatorio,
        execution_timeout=duration(minutes=2)
    )
    
    '''tratamento = PythonOperator(
        task_id='tratamento',
        python_callable=email.tratamento
    )'''
    
    enviaEmail = PythonOperator(
        task_id='enviaEmail',
        python_callable=email.enviaEmail,
        execution_timeout=duration(minutes=3)
    )

    relatorio >> enviaEmail
    #relatorio >> tratamento >> enviaEmail
    