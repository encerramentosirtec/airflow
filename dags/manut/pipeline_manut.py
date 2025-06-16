import sys
import os

PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), "..")
os.chdir(PATH)
sys.path.insert(0, PATH)

import pendulum

from airflow import DAG
from airflow.operators.python import PythonOperator

from src.bots_manut import Bots

bots = Bots()

default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 2,
    'retry_delay' : pendulum.duration(seconds=5)
}

with DAG(
    'pipeline_manut',
    schedule='*/30 6-22 * * *',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    default_view="graph",
    max_active_runs = 1,
    tags = ['manut', 'geoex']
):
    

    atualiza_hro = PythonOperator(
                                task_id='atualiza_hro',
                                python_callable=bots.atualizar_base_hro
    )

    atualiza_medicoes = PythonOperator(
                                    task_id='atualiza_medicoes',
                                    python_callable=bots.atualizar_base_medicoes
    )

    atualiza_base_movimentacoes = PythonOperator(
                                task_id='atualiza_base_movimentacoes',
                                python_callable=bots.atualizar_base_movimentacao
                            )
    
    atualiza_pastas = PythonOperator(
                            task_id='atualiza_pastas',
                            python_callable=bots.atualizar_base_envio_pastas_consulta
                            )

    cria_hros = PythonOperator(
                    task_id='cria_hros',
                    python_callable=bots.criar_hros
                )
    

    cria_pastas = PythonOperator(
                    task_id='cria_pastas',
                    python_callable=bots.criar_pastas
                )

    aceita_hros = PythonOperator(
                    task_id='aceita_hros',
                    python_callable=bots.aceitar_hros
                )

    '''confere_arquivos = PythonOperator(
                    task_id='confere_arquivos',
                    python_callable=bots.conferir_arquivos
                )'''



    atualiza_hro >> cria_hros
    atualiza_medicoes >> cria_hros
    atualiza_base_movimentacoes >> cria_hros
    atualiza_pastas >> cria_hros

    atualiza_hro >> cria_pastas
    atualiza_medicoes >> cria_pastas
    atualiza_base_movimentacoes >> cria_pastas
    atualiza_pastas >> cria_pastas

    atualiza_hro >> aceita_hros