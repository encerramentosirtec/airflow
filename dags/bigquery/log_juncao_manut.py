from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import numpy as np
import hashlib
import pandas as pd
from datetime import datetime, timedelta
import pendulum
import os
import sys

PATH = os.getenv('AIRFLOW_HOME')
# os.chdir(PATH)
sys.path.insert(0, PATH)

from src.bigquery import BigQuery
CLIENT_BIGQUERY = BigQuery()

from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('sirtec-bot.json')

import src.spreadsheets as sh


TABELA = 'sirtec-472112.logs.log_juncao_manut'



def excel_date_to_text(excel_serial, fmt="%d/%m/%Y"):
    try:
        if pd.isna(excel_serial):  # se for NaN
            return np.nan
        excel_serial = int(excel_serial)
        # ignora valores muito baixos ou muito altos (antes de 1900 ou depois de 2100, por ex.)
        if excel_serial <= 0 or excel_serial > 60000:  
            return np.nan
        base_date = datetime(1899, 12, 30)
        date = base_date + timedelta(days=excel_serial)
        return date.strftime(fmt)
    except Exception:
        return np.nan


def row_hash(row):
    # concatena todos os valores da linha em uma string
    row_str = "|".join(str(v) for v in row.values)
    return hashlib.md5(row_str.encode("utf-8")).hexdigest()


def atualiza_tabela():

    columns={
        'OC/PES': 'oc_pes',
        'Projeto': 'projeto',
        'UTD': 'utd',
        'Data do serviço': 'data_servico',
        'Serviço GPM': 'servico_gpm',
        'Categoria de pagamento': 'categoria_pagamento',
        'Localidade': 'Localidade',
        'Tipo de serviço': 'tipo_servico',
        'Data fechamento': 'data_fechamento',
        'Status recepção': 'status_recepcao',
        'Observações': 'observacoes',
        'Status movimentação dos materiais': 'status_movimentacao_materiais',
        'UAR': 'uar',
        'PRJ': 'prj',
        'ID HRO': 'id_hro',
        'Status HRO': 'status_hro',
        'ID medição': 'id_medicao',
        'Status medição': 'status_medicao',
        'ID envio de pasta': 'id_envio_pasta',
        'Status pasta Geoex': 'status_pasta_geoex',
        'Valor total': 'valor_total',
    }


    df_juncao = GS_SERVICE.le_planilha(sh.MANUT_POSTAGEM, 'Ocorrências')


    # Faz a seleção das colunas
    df_juncao = df_juncao[list(columns.keys())].rename(columns=columns)

    # Converte todas as colunas para string
    df_juncao = df_juncao.astype(str)

    # Converte colunas para tipo de 
    colunas_data = ['data_servico', 'data_fechamento']
    for coluna in colunas_data:
        df_juncao[coluna] = pd.to_numeric(df_juncao[coluna], errors='coerce')

    df_juncao[colunas_data] = df_juncao[colunas_data].map(excel_date_to_text)

    for coluna in colunas_data:
        df_juncao[coluna] = pd.to_datetime(df_juncao[coluna], format='%d/%m/%Y', errors='coerce')


    # Converte colunas numericas
    df_juncao['valor_total'] = pd.to_numeric(df_juncao['valor_total'], errors='coerce')

    
    df_juncao['row_hash'] = df_juncao.apply(row_hash, axis=1)
    df_juncao['data_atualizacao'] = pendulum.now('America/Sao_Paulo')

    print(df_juncao.info())

    CLIENT_BIGQUERY.append_to_bigquery(df_juncao, TABELA)
# def atualiza_tabela():


if __name__ == '__main__':
    atualiza_tabela()

default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 2,
    'retry_delay' : pendulum.duration(seconds=30)
}

with DAG(
    dag_id='atualiza_log_juncao_manut',
    schedule='@daily',
    start_date=datetime(2026, 4, 16, tzinfo=pendulum.timezone("America/Sao_Paulo")),
    tags=['bigquery']
):

    atualizar_tabela = PythonOperator(
        task_id='coleta_dados',
        python_callable=atualiza_tabela
    )


    atualizar_tabela