from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from google.cloud import bigquery
import os
import pandas as pd
import pendulum
import sys
from datetime import datetime


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh  # Arquivo contendo link de todas as planilhas

from src.google_sheets import GoogleSheets # Objeto para interagir com as planilhas google
GS_SERVICE = GoogleSheets('sirtec-bot.json')

from src.google_drive import GoogleDrive
DRIVE = GoogleDrive()

CLIENT_BIGQUERY = bigquery.Client.from_service_account_json(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'))

from src.config import configs as cfg
LOG_TABLE = cfg.log_table

PASTA_DRIVE = 'prj'


def _arquivo_atualizado(nome_arquivo, tabela_log):
    """
        Verifica se um arquivo da pasta do Drive foi modificado após a última atualização registrada da tabela.
    """
    pasta = DRIVE.buscar_pasta_por_nome(PASTA_DRIVE)
    arquivos = DRIVE.listar_arquivos(f"'{pasta}' in parents and name='{nome_arquivo}'")

    if not arquivos:
        raise FileNotFoundError(f"Arquivo '{nome_arquivo}' não encontrado na pasta '{PASTA_DRIVE}'.")

    ultima_atualizacao_arquivo = pendulum.parse(arquivos[0]['modifiedTime'])

    query = f"""
        SELECT data_atualizacao
        FROM `{LOG_TABLE}`
        WHERE tabela_atualizada = '{tabela_log}'
        ORDER BY data_atualizacao DESC
        LIMIT 1
    """
    job = CLIENT_BIGQUERY.query(query)
    result = list(job.result())
    ultima_atualizacao_base = pendulum.instance(result[0].data_atualizacao) if result else None

    print(f"Última atualização da base ({tabela_log}):", ultima_atualizacao_base)
    print(f"Última atualização do arquivo ({nome_arquivo}):", ultima_atualizacao_arquivo)

    return ultima_atualizacao_base is None or ultima_atualizacao_arquivo > ultima_atualizacao_base


def verifica_alteracao_prj(**context):
    return _arquivo_atualizado('prj.csv', 'BASE_PRJ')


def verifica_alteracao_correcoes(**context):
    return _arquivo_atualizado('correcoes.csv', 'BASE_CORREÇÕES')


def _corrigir_csv(linha):
    n_cols = 5   # número correto de colunas (igual ao cabeçalho)
    idx = 12      # posição (começando em 0) da coluna problemática

    excesso = len(linha) - n_cols
    return linha[:idx] + [';'.join(linha[idx:idx + excesso + 1])] + linha[idx + excesso + 1:]


def atualizar_base_prj():
    """
        Atualiza a aba BASE_PRJ com as colunas A, L e T do arquivo prj.csv.
    """
    try:
        pasta = DRIVE.buscar_pasta_por_nome(PASTA_DRIVE)
        # DRIVE.baixar_arquivo('prj.csv', arquivo_id=pasta)

        df = pd.read_csv(os.path.join(PATH, 'downloads/prj.csv'), sep=';', on_bad_lines=_corrigir_csv, encoding='utf-8', engine='python')
        df = df.iloc[:, [0, 11, 19]]  # Colunas A, L e T

        GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_PRJ, aba='BASE_PRJ', df=df.fillna(''))

    except Exception as e:
        raise e


def atualizar_base_correcoes():
    """
        Atualiza a aba BASE_CORREÇÕES com as colunas A e L do arquivo correcoes.csv.
    """
    try:
        pasta = DRIVE.buscar_pasta_por_nome(PASTA_DRIVE)
        DRIVE.baixar_arquivo('correcoes.csv', arquivo_id=pasta)

        df = pd.read_csv(os.path.join(PATH, 'downloads/correcoes.csv'), sep=';', on_bad_lines=_corrigir_csv, encoding='utf-8', engine='python')
        df = df.iloc[:, [0, 11]]  # Colunas A e L

        GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_PRJ, aba='BASE_CORREÇÕES', df=df.fillna(''))

    except Exception as e:
        raise e


def log_atualizacao_prj():
    query = f"""
        INSERT INTO `{LOG_TABLE}` (dag_id, data_atualizacao, tabela_atualizada)
        VALUES ('atualizar_prj', CURRENT_TIMESTAMP(), 'BASE_PRJ')
    """
    CLIENT_BIGQUERY.query(query).result()
    print("Log de atualização inserido (BASE_PRJ).")


def log_atualizacao_correcoes():
    query = f"""
        INSERT INTO `{LOG_TABLE}` (dag_id, data_atualizacao, tabela_atualizada)
        VALUES ('atualizar_prj', CURRENT_TIMESTAMP(), 'BASE_CORREÇÕES')
    """
    CLIENT_BIGQUERY.query(query).result()
    print("Log de atualização inserido (BASE_CORREÇÕES).")


default_args = {
    'depends_on_past' : False,
    'owner' : 'hugo',
    'retries' : 1,
    'retry_delay' : pendulum.duration(seconds=60)
}


if __name__ == '__main__':
    atualizar_base_prj()
    log_atualizacao_prj()
    atualizar_base_correcoes()
    log_atualizacao_correcoes()


with DAG(
    dag_id='atualizar_prj',
    tags=['manut'],
    schedule='*/1 7-22 * * *',
    default_args=default_args,
    start_date=datetime(2026, 4, 16, tzinfo=pendulum.timezone("America/Sao_Paulo")),
    max_active_runs=1
):

    checar_alteracao_prj = ShortCircuitOperator(
        task_id='checar_alteracao_prj',
        python_callable=verifica_alteracao_prj,
    )

    atualiza_prj = PythonOperator(
        task_id='atualiza_prj',
        python_callable=atualizar_base_prj,
    )

    log_prj = PythonOperator(
        task_id='log_prj',
        python_callable=log_atualizacao_prj,
        trigger_rule='all_success',
    )

    checar_alteracao_correcoes = ShortCircuitOperator(
        task_id='checar_alteracao_correcoes',
        python_callable=verifica_alteracao_correcoes,
    )

    atualiza_correcoes = PythonOperator(
        task_id='atualiza_correcoes',
        python_callable=atualizar_base_correcoes,
    )

    log_correcoes = PythonOperator(
        task_id='log_correcoes',
        python_callable=log_atualizacao_correcoes,
        trigger_rule='all_success',
    )

    checar_alteracao_prj >> atualiza_prj >> log_prj
    checar_alteracao_correcoes >> atualiza_correcoes >> log_correcoes
