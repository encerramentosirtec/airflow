from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from google.cloud import bigquery
import pendulum
from datetime import datetime, timedelta
import hashlib


import sys
import os
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

from src.google_sheets import GoogleSheets
GSPREAD = GoogleSheets(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'))

CLIENT_BIGQUERY = bigquery.Client.from_service_account_json(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'), project='sirtec-bot')

import src.spreadsheets as sh


TABELA_STAGED = 'sirtec-472112.encerramento.staged_pedidos_faturados'
TABELA_PRINCIPAL = 'sirtec-472112.encerramento.pedidos_faturados'



## Funções auxiliares
def excel_date_to_text(excel_serial, fmt="%d/%m/%Y"):
    # Excel conta a partir de 1900-01-01
    base_date = datetime(1899, 12, 30)  # compensação pelo bug de 1900
    date = base_date + timedelta(days=excel_serial)
    return date.strftime(fmt)

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
    df_pedidos = GSPREAD.le_planilha(sh.PEDIDOS, 'PEDIDOS JULIANA', "A:O", 'UNFORMATTED_VALUE')
    df_pedidos = df_pedidos.rename(columns={
        'Nro Pedido': 'num_pedido',
        'Data': 'data',
        'Setor': 'setor',
        'Unidade': 'unidade',
        'Vr Solic': 'valor'
    })
    df_pedidos_grouped = df_pedidos.groupby('num_pedido', as_index=False)[['data', 'setor', 'unidade', 'valor']].agg({
        'data': 'first',
        'unidade': 'first',
        'setor': 'first',
        'valor': 'sum'
    })

    df_pedidos_grouped['data'] = df_pedidos['data'].apply(lambda x: excel_date_to_text(x))
    df_pedidos_grouped['data'] = pd.to_datetime(df_pedidos_grouped['data'], format='%d/%m/%Y')

    df_pedidos_grouped['ciclo'] = '8'
    df_pedidos_grouped['ano'] = '2025'
    df_pedidos_grouped['row_hash'] = df_pedidos_grouped.apply(row_hash, axis=1)

    df_pedidos_grouped['data_atualizacao'] = datetime.now()

    df_pedidos_grouped['ano'] = df_pedidos_grouped['ano'].astype(int)
    df_pedidos_grouped['ciclo'] = df_pedidos_grouped['ciclo'].astype(int)

    ### Atualiza tabela staged
    overwrite_to_bigquery(df_pedidos_grouped, TABELA_STAGED)





def atualiza_tabela_principal():
    ### Query para atualizar a tabela principal
    merge_query = f"""
        MERGE `{TABELA_PRINCIPAL}` principal
        USING `{TABELA_STAGED}` staged
        ON principal.`num_pedido` = staged.`num_pedido`
        WHEN NOT MATCHED THEN
            INSERT ROW
        WHEN MATCHED AND principal.`row_hash` <> staged.`row_hash` THEN
            UPDATE SET
                principal.`num_pedido` = staged.`num_pedido`,
                principal.data = staged.data,
                principal.unidade = staged.unidade,
                principal.setor = staged.setor,
                principal.`valor` = staged.`valor`,
                principal.ciclo = staged.ciclo,
                principal.ano = staged.ano,
                principal.`row_hash` = staged.`row_hash`,
                principal.`data_atualizacao` = staged.`data_atualizacao`
    """

    CLIENT_BIGQUERY.query(merge_query).result()
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
    'atualiza_pedidos_faturados',
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