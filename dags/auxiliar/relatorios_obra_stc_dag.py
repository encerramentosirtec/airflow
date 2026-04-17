from airflow.sdk import DAG
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.smtp.notifications.smtp import send_smtp_notification
from pendulum import duration, timezone#, today
from datetime import datetime

with DAG(
    dag_id="maestrina_stc_ccm",
    start_date=datetime(2026, 4, 16, tzinfo=timezone("America/Sao_Paulo")),
    schedule=None,  # Disparo manual ou via Bot Telegram
    catchup=False,
    tags=["stc", "obra", "relatorios", "sequencial", "geoex"],
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
        task_id="rejeicoes",
        trigger_dag_id="rejeicoes",
        wait_for_completion=True,
        poke_interval=30,
    )

    relatorio_hro = TriggerDagRunOperator(
        task_id="relatorio-hro",
        trigger_dag_id="relatorio-hro",
        wait_for_completion=True,
    )

    sequencia_de_pendencias = TriggerDagRunOperator(
        task_id="sequencia-de-pendencias",
        trigger_dag_id="sequencia-de-pendencias",
        wait_for_completion=True,
    )

    # Definição da Sequência (Pipeline)
    email_gpm >> rejeicoes >> relatorio_hro