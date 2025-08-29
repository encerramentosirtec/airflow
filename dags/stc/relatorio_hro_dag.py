#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
from src.bots_stc import Bots

bot = Bots()

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : False,
    'email_on_retry' : False,
    'owner' : 'stc',
    'retries' : 2,
    'retry_delay' : pendulum.duration(seconds=5)
}

with DAG('relatorio-hro',
        default_args = default_args,
        #default_view="graph",
        start_date=pendulum.today('America/Sao_Paulo'),
        schedule = '0 7-18 * * 1-6',
        max_active_runs = 1,
        tags = ['stc', 'geoex'],
        catchup = False) as dag:
    
    relatorio = PythonOperator(
        task_id='relatorio',
        python_callable=bot.relatorio
    )
    
    tratamento = PythonOperator(
        task_id='tratamento',
        python_callable=bot.tratamento
    )

    relatorio >> tratamento