from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator, ShortCircuitOperator
from google.cloud import bigquery
import os
import pandas as pd
import pendulum 
import sys
from datetime import datetime, timedelta


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh  # Arquivo contendo link de todas as planilhas

from src.google_sheets import GoogleSheets # Objeto para interagir com as planilhas google
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json')

from src.google_drive import GoogleDrive
DRIVE = GoogleDrive()

CLIENT_BIGQUERY = bigquery.Client.from_service_account_json(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'))

from src.config import configs as cfg
LOG_TABLE = cfg.log_table

def verifica_alteracao_arquivos(**context):
    ### Consulta a última atualização dos arquivos no drive
    pasta = DRIVE.buscar_pasta_por_nome('uar')
    query = f"'{pasta}' in parents"
    ultima_atualizacao_arquivo = [x['modifiedTime'] for x in DRIVE.listar_arquivos(query)]
    datetime_ultima_atualizacao_arquivo = [pendulum.parse(x) for x in ultima_atualizacao_arquivo]

    ### Consulta momento da última atualização da base
    query = f"""
        SELECT data_atualizacao
        FROM `{LOG_TABLE}`
        WHERE tabela_atualizada = 'BASE_UAR'
        ORDER BY data_atualizacao DESC
        LIMIT 1
    """
    job = CLIENT_BIGQUERY.query(query)
    result = list(job.result())
    if result:
        ultima_atualizacao_base = pendulum.instance(result[0].data_atualizacao)

    # Não segue com o processo se não houver arquivos na pasta
    if not ultima_atualizacao_arquivo:
        return False

    print("Última atualização da base:", ultima_atualizacao_base)
    print("Últimas atualizações dos arquivos:", ultima_atualizacao_arquivo)

    # Conferir se as datas de atualização são maiores que a última execução
    atualizar = False
    for data_arquivo in datetime_ultima_atualizacao_arquivo:
        if ultima_atualizacao_base is None or data_arquivo > ultima_atualizacao_base:
            atualizar = True
        else:
            return False
    
    if atualizar:
        print("Arquivos foram atualizados. Iniciando atualização da base.")
        return True



def atualizar_base_uar():
    """
        Atualiza a base de UAR
    """
    try:
        # Baixa o arquivo do Google Drive
        pasta_uar = DRIVE.buscar_pasta_por_nome('uar')
        DRIVE.baixar_arquivo('uar_atualizado.txt', arquivo_id=pasta_uar)
        DRIVE.baixar_arquivo('uar_ativo.txt', arquivo_id=pasta_uar)

        # Lê o arquivo baixado
        df_ativo = pd.read_csv(os.path.join(PATH, 'downloads/uar_ativo.txt'), sep='#', encoding='ISO-8859-1', skiprows=3, on_bad_lines='warn')
        df_atualizado = pd.read_csv(os.path.join(PATH, 'downloads/uar_atualizado.txt'), sep='#', encoding='ISO-8859-1', skiprows=3, on_bad_lines='warn')

        # Sequeência de tratamento dos dados
        map_status = {
            'DEVOLVIDO A OS': 'A. Devolvido',
            'INFORMADO': 'B. Informado',  
            'LIBERADO': 'C. Aprovado',
            'FINALIZADO': 'D. Finalizado'
        }

        df = pd.concat([df_atualizado, df_ativo], ignore_index=True)
        print(df.info())
        df['STATUS_HIERARQUICO'] = df['STATUS'].map(map_status)
        df.sort_values(by='STATUS_HIERARQUICO', ascending=True, inplace=True)
        # df.drop_duplicates(subset='ID', inplace=True)

        # Atualiza a planilha
        GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_UAR', df=df.fillna(''))

    except Exception as e:
        raise e


def log_atualização():
    query = f"""
        INSERT INTO `{LOG_TABLE}` (dag_id, data_atualizacao, tabela_atualizada)
        VALUES ('atualiza_uar', CURRENT_TIMESTAMP(), 'BASE_UAR')
    """
    CLIENT_BIGQUERY.query(query).result()
    print("Log de atualização inserido.")


if __name__ == '__main__':
    fake_context = {'prev_data_interval_start_success': '2025-09-12T00:00:00+00:00'}
    verifica_alteracao_arquivos(**fake_context)



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
    dag_id='atualiza_uar',
    tags=['manut'],
    schedule='*/1 7-22 * * *',
    default_args=default_args,
    start_date=pendulum.today('America/Sao_Paulo'),
    max_active_runs=1
):

    checar_alteracao_arquivos = ShortCircuitOperator(
        task_id='checar_alteracao_arquivos',
        python_callable=verifica_alteracao_arquivos,
    )

    atualiza_uar = PythonOperator(
        task_id='atualiza_uar',
        python_callable=atualizar_base_uar,
    )

    log_atualizacao = PythonOperator(
        task_id="log_execution",
        python_callable=log_atualização,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )

    checar_alteracao_arquivos >> atualiza_uar
