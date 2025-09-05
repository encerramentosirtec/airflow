from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
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



def atualizar_base_medicoes():
    map_status = {
        'MPC': 'A. Pedido lançado',
        'MVD': 'B. Validada',
        'MEA': 'C. Atestada',
        'MPA': 'D. Postada',
        'MRJ': 'E. Rejeitada'
    }

    id_relatorio = ID_RELATORIOS.loc[0].ID
    download = GEOEX.baixar_relatorio(id_relatorio)
    if download['sucess']:
        try:
            df = pd.read_csv(os.path.join(PATH, 'downloads/Geoex - Relatório - Acompanhamento - Detalhado.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',', dtype='str')
            
            # Filtrando o dataframe
            df = df[~df['TITULO'].str.startswith(('COBRANCA', 'LIGACAO', 'PERDAS')) & ~df['TITULO'].str.contains('SOLAR', na=False)]

            # Ajustando a coluna 'PROJETO'
            df['PROJETO'] = df['PROJETO'].str.replace('Y-', 'B-', regex=False)

            # Mapeando status
            df['STATUS AJUSTADO'] = df['STATUS'].map(map_status)

            # Extraindo 'OC/PMS'
            df['OC/PMS'] = df.apply(lambda x: re.search(r'\d{4}_\d{1,2}_\d+', x['TITULO']).group(0) if x['POSTAGEM'] == 'GX02 - MEDIÇÃO | HUB REGISTRO OPERACIONAL' and re.search(r'\d{4}_[1-9]\d*_\d+', x['TITULO']) else x['OCORRENCIA'], axis=1)

            # Criando a coluna 'ID_MEDIÇÃO' 
            df['ID_MEDIÇÃO'] = df['PROJETO'] + df['OC/PMS'].astype(str)

            # Ordenando e removendo duplicatas
            df = df.sort_values(by='STATUS AJUSTADO').drop_duplicates(subset='ID_MEDIÇÃO')

            # Agrupando os dados
            df_grouped = df.groupby('ID', as_index=False).agg({
                'PROJETO': 'first',
                'TITULO': 'first',
                'OC/PMS': 'first',
                'STATUS AJUSTADO': 'first',
                'ID_MEDIÇÃO': 'first',
                'VALOR_PREVISTO': 'sum'
            }).sort_values(by='STATUS AJUSTADO', ascending=False)

            sucess = GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_MEDIÇÕES', df=df_grouped)
            if sucess:
                GS_SERVICE.escreve_planilha(url=sh.MANUT_POSTAGEM, aba='Atualizações', df=pd.DataFrame([['Medições', datetime.now().strftime("%d/%m/%Y, %H:%M")]]), range='A4')
                            
            return {
                'status': 'Ok',
                'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
            }
        except Exception as e:
            raise e

    else:
        raise Exception(
            f"""
            Falha ao baixar csv.
            Statuscode: { download['status_code'] }
            Message: { download['data'] }
            """
        )
     
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
    schedule='*/35 6-22 * * *',
    default_args=default_args,
    start_date=pendulum.today('America/Sao_Paulo')
):

    atualizar_medicoes = PythonOperator(
        task_id='atualizar_medicoes',
        python_callable=atualizar_base_medicoes,

    )

    atualizar_medicoes