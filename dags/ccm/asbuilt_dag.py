from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from pendulum import duration, today
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
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    asbuilt = PythonOperator(
        task_id='asbuilt',
        python_callable=bot.asbuilt,
        retries=2,
        retry_delay=duration(seconds=20)
    )

    asbuilt