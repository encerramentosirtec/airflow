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

with DAG('solar',
        default_args = default_args,
        #default_view="graph",
        start_date=pendulum.today('America/Sao_Paulo'),
        schedule = '0,30 7-18,20 * * 1-6',
        tags = ['solar', 'geoex', 'stc'],
        catchup = False) as dag:
    
    solar = PythonOperator(
        task_id='solar',
        python_callable=bot.atualiza_solar
    )
    
    solar