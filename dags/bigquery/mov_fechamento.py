from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import pendulum
from datetime import datetime, timedelta
import hashlib
import numpy as np
import sys
import os

os.environ['AIRFLOW_HOME'] = '/home/hugoviana/airflow_sirtec/airflow'
PATH = os.getenv('AIRFLOW_HOME')

os.chdir(PATH)
sys.path.insert(0, PATH)

from src.google_sheets import GoogleSheets

GSPREAD = GoogleSheets('causal_scarab.json')

CLIENT_BIGQUERY = bigquery.Client.from_service_account_json(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'))

import src.spreadsheets as sh

TABELA_STAGED = 'sirtec-472112.logs.staged_movimentacao_fechamento'
TABELA_PRINCIPAL = 'sirtec-472112.logs.movimentacao_fechamento'



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


def overwrite_to_bigquery(df: pd.DataFrame, table_id: str):
    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # sobrescreve a tabela
    )
    job = CLIENT_BIGQUERY.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()  # espera o job terminar
    print(f"{len(df)} linhas carregadas em {table_id} (sobrescrita).")




def atualiza_tabela_staged():
    colunas = {
        'Unidade': 'unidade',
        'Setor': 'setor',
        'Projeto': 'projeto',
        'Tipo de Mov.': 'tipo_mov',
        'Reserva': 'reserva',
        'Material': 'material',
        'Descrição': 'descricao',
        'Qtd.': 'quantidade',
        'Supervisor': 'supervisor',
        'Análise Almox': 'analise_almox',
        '   Mov. Almox': 'mov_almox',
        'DT Mov. Almox': 'data_mov_almox',
        'Observação Almox': 'obs_almox',
        'Check Fechamento': 'check_fechamento',
        'Observação Fechamento': 'obs_fechamento',
        'DT Solic. Reserva': 'data_solicitacao_reserva',
        'DT Criação Reserva': 'data_criacao_reserva',
        'DT Solic. Mov.': 'data_solicitacao_mov'
    }


    df_mov_fechamento = GSPREAD.le_planilha(sh.ALMOX_V6, 'Consolidado', render_option='UNFORMATTED_VALUE')
    df_mov_fechamento.rename(columns=colunas, inplace=True)

    # Converte todas as colunas para string
    df_mov_fechamento = df_mov_fechamento.astype(str)


    # Converte colunas de data para o tipo correto
    colunas_data = ['data_mov_almox', 'data_solicitacao_reserva', 'data_criacao_reserva', 'data_solicitacao_mov']
    for coluna in colunas_data:
        df_mov_fechamento[coluna] = pd.to_numeric(df_mov_fechamento[coluna], errors="coerce")

    df_mov_fechamento[colunas_data] = df_mov_fechamento[colunas_data].map(excel_date_to_text)

    df_mov_fechamento['data_mov_almox'] = pd.to_datetime(df_mov_fechamento['data_mov_almox'], format='%d/%m/%Y', errors='coerce')
    df_mov_fechamento['data_solicitacao_reserva'] = pd.to_datetime(df_mov_fechamento['data_solicitacao_reserva'], format='%d/%m/%Y', errors='coerce')
    df_mov_fechamento['data_criacao_reserva'] = pd.to_datetime(df_mov_fechamento['data_criacao_reserva'], format='%d/%m/%Y', errors='coerce')
    df_mov_fechamento['data_solicitacao_mov'] = pd.to_datetime(df_mov_fechamento['data_solicitacao_mov'], format='%d/%m/%Y', errors='coerce')


    # Converte colunas numericas
    df_mov_fechamento['quantidade'] = pd.to_numeric(df_mov_fechamento['quantidade'].replace(',', '.'), errors='coerce')


    # Converte colunas booleanas
    df_mov_fechamento['mov_almox'] = df_mov_fechamento['mov_almox'].astype(bool)


    # Cria coluna hash
    df_mov_fechamento['row_hash'] = df_mov_fechamento.apply(row_hash, axis=1)


    # Registra data de atualização
    df_mov_fechamento['data_atualizacao'] = pendulum.now('America/Sao_Paulo')
    overwrite_to_bigquery(df_mov_fechamento, 'sirtec-472112.logs.staged_movimentacao_fechamento')



def atualiza_tabela_principal():
    merge_query = f"""
        MERGE `{TABELA_PRINCIPAL}` principal
        USING `{TABELA_STAGED}` staged
        ON principal.row_hash = staged.row_hash
        WHEN NOT MATCHED THEN
            INSERT ROW
        """

    job = CLIENT_BIGQUERY.query(merge_query).result()
    print("ID do Job:", job.job_id)
    print("Bytes processados:", job.total_bytes_processed)
    print("Linhas afetadas:", job.num_dml_affected_rows)

    print(f"Tabela {TABELA_PRINCIPAL} atualizada.")





default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 0,
    'retry_delay' : pendulum.duration(seconds=30)
}

with DAG(
    'atualiza_mov_fechamento',
    schedule='*/60 6-23 * * 1-6',
    start_date=pendulum.today('America/Sao_Paulo'),
    tags=['bigquery']
):
    
    atualizar_staged = PythonOperator(
        task_id='atualizar_staged',
        python_callable=atualiza_tabela_staged
    )

    atualizar_principal = PythonOperator(
        task_id='atualizar_principal',
        python_callable=atualiza_tabela_principal
    )


    atualizar_staged >> atualizar_principal