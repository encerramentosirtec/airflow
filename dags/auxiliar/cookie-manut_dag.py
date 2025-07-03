#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from src.bot_telegram import Bots
from pendulum import today, duration

bot = Bots()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 2,
    'owner' : 'heli',
    'retry_delay' : duration(seconds=5)
}

with DAG('cookie-manut',
        default_args = default_args,
        #default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = None,
        tags = ['manut', 'cookie', 'aux'],
        catchup = False) as dag:
    
    push_cookie = PythonOperator(
        task_id = 'push_cookie',
        python_callable = bot.push_cookie
    )

    push_cookie