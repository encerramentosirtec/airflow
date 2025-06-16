from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from src.bot_telegram import Bots
from datetime import datetime

bot = Bots()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli'
}

with DAG('bot-telegram',
        default_args = default_args,
        default_view="graph",
        start_date=datetime(2024,12,1),
        schedule_interval = None,
        max_active_runs = 1,
        tags = ['manut', 'telegram'],
        catchup = False) as dag:
    
    run_bot = PythonOperator(
        task_id = 'run_bot',
        python_callable = bot.run_bot
    )

    run_bot