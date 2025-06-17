from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone, duration, today
from src.bots_ccm import Bots

bot = Bots()
br_tz = timezone("Brazil/East")

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 2,
    'owner' : 'bob'
}

with DAG('atualizarzps09',
        default_args = default_args,
        #default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0 8,11,13,15 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    atualizarzps09 = PythonOperator(
        task_id='atualizarzps09',
        python_callable=bot.atualiza_zps09,
        retries=2,
        retry_delay=duration(seconds=20)
    )

    atualizarzps09