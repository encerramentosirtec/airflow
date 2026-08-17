#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
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
        schedule = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    consulta_asbuilt = PythonOperator(
        task_id='consulta_asbuilt',
        python_callable=bot.asbuilt,
        retries=2,
        retry_delay=duration(seconds=20),
        execution_timeout=duration(minutes=10)
    )

    escreve_asbuilt = PythonOperator(
        task_id='escreve_asbuilt',
        python_callable=bot.escreve_asbuilt,
        retries=2,
        retry_delay=duration(seconds=20),
        execution_timeout=duration(minutes=10)
    )

    atualiza_municipios = PythonOperator(
        task_id='atualiza_municipios',
        python_callable=bot.atualiza_municipios,
        retries=2,
        retry_delay=duration(seconds=20),
        execution_timeout=duration(minutes=10)
    )

    consulta_asbuilt >> escreve_asbuilt >> atualiza_municipios