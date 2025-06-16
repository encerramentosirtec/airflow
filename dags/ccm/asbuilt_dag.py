from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone, duration, today
from src.bots_ccm import Bots
import sys
import os

br_tz = timezone("Brazil/East")

PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), "../..")
os.chdir(PATH)
sys.path.insert(0, PATH)
bot = Bots()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'bob'
}

with DAG('asbuilt',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule_interval = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
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