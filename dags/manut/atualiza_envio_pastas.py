from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import pendulum 
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



def baixar_arquivo_geoex():
    # Consulta id do relatorio
    id_relatorio = ID_RELATORIOS.loc[1].ID

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


def atualizar_base():

    ### Leitura e tratamento dos dados
    df = pd.read_csv(
            os.path.join(PATH, 'downloads/Geoex - Acomp - Envio de pastas - Consulta.csv'),
            encoding='ISO-8859-1',
            sep=';',
            parse_dates=['DATA_SOLICITACAO'], 
            dayfirst=True
        )
    
    df['PROJETO'] = df['PROJETO'].str.replace('Y-', 'B-')

    data_corte = pd.Timestamp(day=1, month=4, year=2024)
    df = df.query("DATA_SOLICITACAO > @data_corte")

    # map_status_ajustado = {
    #     'ACEITO COM RESTRIÇÕES': 'A. Aceita',
    #     'ACEITO': 'A. Aceita',
    #     'VALIDADO': 'B. Validada',
    #     'CRIADO': 'C. Enviada',
    #     'REJEITADO': 'D. Rejeitada',
    #     'CANCELADO': 'E. Cancelada'
    # }
    # df['STATUS AJUSTADO'] = df['STATUS'].map(lambda x: map_status_ajustado[x])

    df = df.sort_values(['DATA_SOLICITACAO'], ascending=False)

    df = df.drop_duplicates(subset='PROJETO')

    df['ID_USUARIO_SOLICITANTE'] = df['USUARIO_SOLICITACAO'].str.extract(r'([a-zA-Z]{1,3}\d{6})')
    df = df.query("ID_USUARIO_SOLICITANTE != 'ORC796500'")

    df['DATA_SOLICITACAO'] = pd.to_datetime(df['DATA_SOLICITACAO']).dt.strftime('%d/%m/%Y')


    ### Atualização da base
    sucess = GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_ENVIO_PASTAS', df=df.fillna(""))
    if sucess:
        GS_SERVICE.escreve_planilha(url=sh.MANUT_POSTAGEM, aba='Atualizações', df=pd.DataFrame([['Envio de pastas', datetime.now().strftime("%d/%m/%Y, %H:%M")]]), range='A5')
    else:
        raise Exception(
            f"""
            Falha ao atualizar base.
            "{ sucess }"
            """
        )


    return {
        'status': 'Ok',
        'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
    }





default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 0,
    'retry_delay' : pendulum.duration(seconds=5)
}


with DAG(
    'atualiza_envio_pastas',
    schedule='*/25 6-22 * * *',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    #default_view="graph",
    max_active_runs = 1,
    tags = ['manut', 'geoex']
):
    
    baixar_relatorio = PythonOperator(
        task_id='baixar_relatorio',
        python_callable=baixar_arquivo_geoex
    )

    atualiza_pastas = PythonOperator(
        task_id='atualiza_pastas',
        python_callable=atualizar_base
    )



    baixar_relatorio >> atualiza_pastas