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
    'owner' : 'bob',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('lv',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    lv = PythonOperator(
        task_id='lv',
        python_callable=bot.lv_geral
    )

    lv