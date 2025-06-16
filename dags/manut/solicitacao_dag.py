from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from src.bots_manut import Bots
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

with DAG('solicitacoes-de-reservas',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule_interval = '0 6,12 * * 1-6',
        max_active_runs = 1,
        tags = ['reservas', 'geoex', 'manut'],
        catchup = False) as dag:

    solicitacoes = PythonOperator(
        task_id = 'solicitacoes',
        python_callable = bot.atualiza_solicitacoes
    )

    solicitacoes