from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from google.cloud import bigquery
from datetime import datetime
import os
import pandas as pd
import pendulum 
import re
import sys


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh  # Arquivo contendo link de todas as planilhas

from src.geoex import Geoex # Objeto para interagir com o Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.google_sheets import GoogleSheets # Objeto para interagir com as planilhas google
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json')

ID_RELATORIOS = GS_SERVICE.le_planilha(url=sh.ID_RELATORIOS, aba='id_relatorios_geoex') # PLanilha contendo Id dos relatórios baixados no Geoex

CLIENT_BIGQUERY = bigquery.Client.from_service_account_json(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'))

from src.config import configs as cfg
LOG_TABLE = cfg.log_table


def baixar_arquivo_geoex():
    id_relatorio = ID_RELATORIOS.loc[0].ID

    download = GEOEX.baixar_relatorio(id_relatorio)

    if download['sucess']:
        print("Download realizado com sucesso!")
    else:
        raise Exception(
            f"""
            Falha ao baixar csv.
            Statuscode: { download['status_code'] }
            Message: { download['data'] }
            """
        )



def atualizar_base_medicoes():
    map_status = {
        'MPC': 'A. Pedido lançado',
        'MVD': 'B. Validada',
        'MEA': 'C. Atestada',
        'MPA': 'D. Postada',
        'MRJ': 'E. Rejeitada',
    }

    ### Leitura e tratamento dos dados
    df = pd.read_csv(os.path.join(PATH, 'downloads/Geoex - Relatório - Acompanhamento - Detalhado.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',')
    
    # Filtrando o dataframe
    df = df[~df['TITULO'].str.startswith(('COBRANCA', 'LIGACAO', 'PERDAS')) & ~df['TITULO'].str.contains('SOLAR', na=False)]

    # Ajustando a coluna 'PROJETO'
    df['PROJETO'] = df['PROJETO'].str.replace('Y-', 'B-', regex=False)

    # Mapeando status
    df['STATUS AJUSTADO'] = df['STATUS'].map(map_status)

    # Extraindo 'OC/PMS'
    # df['OC/PMS'] = df.apply(lambda x: re.search(r'\d{4}_\d{1,2}_\d+', x['TITULO']).group(0) if x['POSTAGEM'] == 'GX02 - MEDIÇÃO | HUB REGISTRO OPERACIONAL' and re.search(r'\d{4}_[1-9]\d*_\d+', x['TITULO']) else x['OCORRENCIA'], axis=1)

    # Criando coluna com OC ou OS
    df['OC'] = df.apply(lambda x: x['OCORRENCIA'] if pd.isna(x['ORDEM_SERVICO']) else x['ORDEM_SERVICO'], axis=1)


    ##### INCLUIR NO AGRUPAMENTO O NUMERO DA OS #####

    # Criando a coluna 'ID_MEDIÇÃO' 
    df['ID_MEDIÇÃO'] = df['PROJETO'] + df['OC'].astype(str)

    # Agrupando os dados
    df_grouped = df.groupby('ID', as_index=False).agg({
        'PROJETO': 'first',
        'TITULO': 'first',
        'OC': 'first',
        'STATUS AJUSTADO': 'first',
        'ID_MEDIÇÃO': 'first',
        'VALOR_PREVISTO': 'sum'
    }).sort_values(by=['STATUS AJUSTADO', 'ID'], ascending=[True, False])


    ### Atualização da base
    GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_MEDIÇÕES', df=df_grouped)
                    

    

def log_atualização():
        query = f"""
            INSERT INTO `{LOG_TABLE}` (dag_id, data_atualizacao, tabela_atualizada)
            VALUES ('atualizar_medicoes', CURRENT_TIMESTAMP(), 'BASE_MEDICOES')
        """
        CLIENT_BIGQUERY.query(query).result()
        print("Log de atualização inserido.")
     

if __name__ == "__main__":
    atualizar_base_medicoes()


default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 1,
    'retry_delay' : pendulum.duration(seconds=60)
}

with DAG(
    dag_id='atualizar_medicoes',
    tags=['manut', 'geoex'],
    schedule='*/30 6-22 * * *',
    default_args=default_args,
    start_date=pendulum.today('America/Sao_Paulo')
):

    baixar_relatorio = PythonOperator(
        task_id='baixar_relatorio',
        python_callable=baixar_arquivo_geoex
    )

    atualizar_medicoes = PythonOperator(
        task_id='atualizar_medicoes',
        python_callable=atualizar_base_medicoes,

    )

    log_atualizacao = PythonOperator(
        task_id="log_execution",
        python_callable=log_atualização,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )

    baixar_relatorio >> atualizar_medicoes >> log_atualizacao