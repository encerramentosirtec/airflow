from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import os
import pandas as pd
import pendulum 
import sys
from time import sleep


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh  # Arquivo contendo link de todas as planilhas

from src.google_sheets import GoogleSheets # Objeto para interagir com as planilhas google
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json')

from src.google_drive import GoogleDrive
DRIVE = GoogleDrive()



def atualizar_base_uar():
    """
        Atualiza a base de UAR
    """
    try:
        # Baixa o arquivo do Google Drive
        drive = GoogleDrive()
        drive.baixar_arquivo('uar_atualizado.txt')
        drive.baixar_arquivo('uar_ativo.txt')

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
    schedule='0 8,15 * * 1-5',
    default_args=default_args,
    start_date=pendulum.today('America/Sao_Paulo')
):

    atualiza_uar = PythonOperator(
        task_id='atualiza_uar',
        python_callable=atualizar_base_uar,

    )

    atualiza_uar
