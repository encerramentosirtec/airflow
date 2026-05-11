from airflow.sdk import DAG
#from airflow.operators.empty import EmptyOperator
#from airflow.operators.python import BranchPythonOperator
#from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from pendulum import duration, timezone
from datetime import datetime

default_args = {
    'depends_on_past' : False,
    'retries' : 3,
    'owner' : 'heli',
    'retry_delay' : duration(seconds=5)
}

with DAG('controladora_relatorios_manut',
        default_args = default_args,
        start_date=datetime(2026, 4, 17, tzinfo=timezone("America/Sao_Paulo")),
        schedule = '0/20 7-22 * * 1-6',
        tags = ['aux', 'relatorios', 'manut', 'geoex'],
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
    
    pastas = TriggerDagRunOperator(
        task_id="pastas_trigger",
        trigger_dag_id="atualiza_envio_pastas",
        wait_for_completion=True,
        poke_interval=30,
        deferrable=True
    )
    
    hro = TriggerDagRunOperator(
        task_id="hro_trigger",
        trigger_dag_id="atualizar_hro",
        wait_for_completion=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )
    
    medições = TriggerDagRunOperator(
        task_id="medições_trigger",
        trigger_dag_id="atualizar_medicoes",
        wait_for_completion=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )
    
    pedidos_criado = TriggerDagRunOperator(
        task_id="pedidos_criado_trigger",
        trigger_dag_id="atualiza_pedidos_criados",
        wait_for_completion=True,
        poke_interval=30,
        trigger_rule='none_failed_min_one_success'
    )

    pastas >> hro >> medições >> pedidos_criado