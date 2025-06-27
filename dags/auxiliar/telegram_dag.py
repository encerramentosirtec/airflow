from airflow.models.dag import DAG
#from airflow.sdk import DAG
from airflow.operators.python import PythonOperator
#from airflow.providers.standard.operators.python import PythonOperator
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

with DAG('bot-telegram',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = None,
        max_active_runs = 1,
        tags = ['manut', 'telegram', 'aux'],
        catchup = False) as dag:
    
    run_bot = PythonOperator(
        task_id = 'run_bot',
        python_callable = bot.run_bot
    )

    run_bot