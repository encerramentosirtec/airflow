from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import os
import pandas as pd
import pendulum 
import sys
import glob

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
    id_relatorios = []
    id_relatorios.append([ID_RELATORIOS.loc[3].ID, 'Geoex - Processos com HRO - Consulta - Ocorrências.csv'])
    id_relatorios.append([ID_RELATORIOS.loc[5].ID, 'Geoex - Processos com HRO - Consulta - Projeto filho.csv'])

    for id in id_relatorios:
        download = GEOEX.baixar_relatorio(id[0], name=id[1], file_path='downloads/hros')
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
        

def atualizar_base_hro():

    ### Leitura e tratamento dos dados
    arquivos = glob.glob(os.path.join(PATH, 'downloads/hros/*.csv'))
    for arquivo in arquivos:
        df = pd.read_csv(
            os.path.join(PATH, arquivo),
            encoding='ISO-8859-1',
            sep=';',
            dayfirst=True
        )

        map_status_hro = {
            'ACEITO': 'A. Aceito',
            'ANALISADO': 'B. Analisado',
            'EM ANÁLISE': 'C. Em análise',
            'ANÁLISE CANCELADA': 'D. Análise cancelada',
            'VALIDADO': 'E. Validado',
            'VALIDANDO': 'F. Validando',
            'VALIDAÇÃO CANCELADA': 'G. Validação cancelada',
            'ENVIADO': 'H. Enviado',
            'CRIADO': 'I. Criado',
            'ENVIO CANCELADO': 'J. Envio cancelado',
            'REJEITADO': 'K. Rejeitado',
            'EXPURGADO': 'L. Expurgado',
            'CANCELADO': 'M. Cancelado',
        }

        map_status_medicao = {
            'MPC': 'A. Pedido lançado',
            'MVD': 'B. Validada',
            'MEA': 'C. Atestada',
            'MPA': 'D. Postada',
            'MRJ': 'E. Rejeitada'
        }
        
        df['PROJETO'] = df['PROJETO'].str.replace('Y-', 'B-')
        df['ID'] = df['PROJETO'] + df['PROCESSO']
        df['STATUS_HIERARQUICO'] = df['STATUS'].map(map_status_hro)
        df['STATUS_MEDICAO_HIERARQUICO'] = df['STATUS.1'].map(map_status_medicao)


        # df = df.sort_values('STATUS_HIERARQUICO', ascending=True)
        # df.drop_duplicates('PROCESSO', inplace=True)

        # atualizar df_att com os valores de df
        df_att = pd.concat([df_att, df], ignore_index=True)

        
    
    ### Atualização da base
    df_att.sort_values(by='STATUS_HIERARQUICO', inplace=True, ascending=True)
    sucess = GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_HRO', df=df_att.fillna(""))
    if sucess:
        GS_SERVICE.escreve_planilha(url=sh.MANUT_POSTAGEM, aba='Atualizações', df=pd.DataFrame([['Base HRO', datetime.now().strftime("%d/%m/%Y, %H:%M")]]), range='A3', input_option='USER_ENTERED')
    else:
        raise Exception(
            f"""
            Falha ao atualizar base.
            "{ sucess }
            """
        )

    return {
        'status': 'Ok',
        'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
    }





def aceitar_hros():
    base_hro = GS_SERVICE.le_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_HRO')
    hros = base_hro.query("STATUS == 'ANALISADO'")['PROTOCOLO']
    for hro in hros:
        r = GEOEX.aceitar_hro(hro)
        print(hro)
        print(r['data'])





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
    dag_id='atualizar_hro',
    tags=['manut', 'geoex'],
    schedule='*/30 6-22 * * *',
    default_args=default_args,
    start_date=pendulum.today('America/Sao_Paulo')
):

    baixar_relatorio = PythonOperator(
        task_id='baixar_relatorio',
        python_callable=baixar_arquivo_geoex
    )

    atualizar_hro = PythonOperator(
        task_id='atualiza_hro',
        python_callable=atualizar_base_hro,

    )

    aceitar_hro = PythonOperator(
        task_id='aceitar_hro',
        python_callable=aceitar_hros,

    )

    baixar_relatorio >> atualizar_hro >> aceitar_hro
