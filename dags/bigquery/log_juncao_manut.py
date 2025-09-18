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

DF = pd.DataFrame()




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


def log_juncao():

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


    DF = GS_SERVICE.le_planilha(sh.MANUT_POSTAGEM, 'Ocorrências')


    # Faz a seleção das colunas
    DF = DF[list(columns.keys())].rename(columns=columns)

    # Converte todas as colunas para string
    DF = DF.astype(str)

    # Converte colunas para tipo de 
    colunas_data = ['data_servico', 'data_fechamento']
    for coluna in colunas_data:
        DF[coluna] = pd.to_numeric(DF[coluna], errors='coerce')

    DF[colunas_data] = DF[colunas_data].map(excel_date_to_text)

    for coluna in colunas_data:
        DF[coluna] = pd.to_datetime(DF[coluna], format='%d/%m/%Y', errors='coerce')


    # Converte colunas numericas
    DF['valor_total'] = pd.to_numeric(DF['valor_total'], errors='coerce')

    
    DF['row_hash'] = DF.apply(row_hash, axis=1)
    DF['data_atualizacao'] = pendulum.now('America/Sao_Paulo')



def atualiza_tabela():
    CLIENT_BIGQUERY.append_to_bigquery(DF, TABELA)


if __name__ == '__main__':
    log_juncao()
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
    start_date=pendulum.today('America/Sao_Paulo'),
    tags=['bigquery']
):

    coletar_dados = PythonOperator(
        task_id='coleta_dados',
        python_callable=log_juncao
    )

    atualizar_tabela = PythonOperator(
        task_id='atualiza_tabela',
        python_callable=atualiza_tabela
    )


    coletar_dados >> atualizar_tabela