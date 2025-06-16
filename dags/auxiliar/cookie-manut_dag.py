from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from src.bot_telegram import push_cookie
from datetime import datetime

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli'
}

with DAG('cookie-manut',
        default_args = default_args,
        default_view="graph",
        start_date=datetime(2024,12,1),
        schedule_interval = None,
        tags = ['manut', 'cookie'],
        catchup = False) as dag:
    
    push_cookie = PythonOperator(
        task_id = 'push_cookie',
        python_callable = push_cookie
    )

    push_cookie