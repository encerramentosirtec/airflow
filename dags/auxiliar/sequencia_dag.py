from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator#, BranchPythonOperator
from src.bots_auxiliar import Bots
from pendulum import today, duration

bot = Bots()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('sequencia-de-pendencias',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule_interval = '0,15,30,45 7,18 * * 1-6',
        max_active_runs = 1,
        tags = ['sequencia', 'gpm'],
        catchup = False) as dag:

    sequencia = PythonOperator(
        task_id = 'sequencia',
        python_callable = bot.atualiza_sequencia
    )

    sequencia
    