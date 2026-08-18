#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
import pendulum
from src.bots_auxiliar import Bots_aux
from datetime import datetime

bot = Bots_aux()

default_args = {
    'depends_on_past' : False,
    'owner' : 'stc',
    'retries' : 2,
    'retry_delay' : pendulum.duration(seconds=5)
}

with DAG('relatorio-reservas',
        default_args = default_args,
        #default_view="graph",
        start_date=datetime(2026, 4, 16, tzinfo=pendulum.timezone("America/Sao_Paulo")),
        schedule = None,#'0 7-18 * * 1-6',
        max_active_runs = 1,
        tags = ['stc', 'geoex'],
        catchup = False,) as dag:
    
    relatorio = PythonOperator(
        task_id='relatorio',
        python_callable=bot.relatorio_reservas,
        execution_timeout=pendulum.duration(minutes=5)
    )
    
    tratamento = PythonOperator(
        task_id='tratamento',
        python_callable=bot.salvar_reservas,
        execution_timeout=pendulum.duration(minutes=2)
    )

    relatorio >> tratamento