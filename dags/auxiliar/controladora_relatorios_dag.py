
from airflow.sdk import DAG
from airflow.operators.empty import EmptyOperator
#from airflow.providers.standard.operators.empty.EmptyOperator import EmptyOperator
from airflow.operators.python import BranchPythonOperator
#from airflow.providers.standard.operators.python.BranchPythonOperator import BranchPythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import duration, timezone, now
from datetime import datetime

def verifica_horario():
    # Define o fuso horário de Brasília
    agora = now('America/Sao_Paulo')
    horarios = [8,11,14,17]
    
    # Exemplo: Só permite rodar entre 08:00 e 18:00
    if agora.hour in horarios and agora.minute < 10:
        return 'rejeicoes_trigger'
    else:
        return 'pular'

default_args = {
    'depends_on_past' : False,
    'retries' : 3,
    'owner' : 'heli',
    'retry_delay' : duration(seconds=5)
}

with DAG('controladora_relatorios',
        default_args = default_args,
        start_date=datetime(2026, 4, 17, tzinfo=timezone("America/Sao_Paulo")),
        schedule = '0/10 7-18 * * 1-6',
        tags = ['aux', 'relatorios', 'stc', 'email', 'geoex'],
        catchup = False,
        on_failure_callback=[
            send_smtp_notification(
                from_email="sirtec.heli@gmail.com",
                to="heli.silva@sirtec.com.br",
                subject="[Error] The dag {{ dag.dag_id }} failed",
                html_content="debug logs",
            )
        ],
        ) as dag:
    
    rejeicoes = TriggerDagRunOperator(
        task_id="rejeicoes_trigger",
        trigger_dag_id="rejeicoes",         # ID exato da DAG de coleta
        wait_for_completion=True,           # ESSENCIAL: Espera terminar para seguir
        poke_interval=30,                   # Verifica o status a cada 30s
        deferrable=True
    )
    
    '''relatorio_hro = TriggerDagRunOperator(
        task_id="relatorio_hro_trigger",
        trigger_dag_id="relatorio-hro",  # ID exato da DAG de coleta
        wait_for_completion=True,           # ESSENCIAL: Espera terminar para seguir
        poke_interval=30,                   # Verifica o status a cada 30s
        trigger_rule='none_failed_min_one_success'
    )'''
    
    relatorio_reservas = TriggerDagRunOperator(
        task_id="relatorio_reservas_trigger",
        trigger_dag_id="relatorio-reservas",  # ID exato da DAG de coleta
        wait_for_completion=True,           # ESSENCIAL: Espera terminar para seguir
        poke_interval=30,                   # Verifica o status a cada 30s
        trigger_rule='none_failed_min_one_success'
    )

    checar_hora = BranchPythonOperator(
        task_id='checar_horario_email',
        python_callable=verifica_horario
    )

    pular = EmptyOperator(task_id='pular')

    checar_hora >> [rejeicoes, pular]
    [rejeicoes, pular] >> relatorio_reservas# >> relatorio_hro