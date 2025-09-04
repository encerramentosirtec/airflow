from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator


import os
import sys
import pandas as pd
import pendulum

PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh

from src.geoex import Geoex
GEOEX = Geoex('cookie_hugo.json')


from src.google_sheets import GoogleSheets # Objeto para interagir com as planilhas google
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json')

ID_RELATORIOS = GS_SERVICE.le_planilha(url='https://docs.google.com/spreadsheets/d/1VFxxABMX1WQDbYFll2nO5CEp2FGuNpIqTMftncVRpic/edit', aba='id_relatorios_geoex') # Planilha contendo Id dos relatórios baixados no Geoex



def atualiza_pedidos_criados():
    id_relatorio = ID_RELATORIOS.query("Relatório == 'Pedidos criados'")['ID'].values[0]
    arquivo = GEOEX.baixar_relatorio(id_relatorio, name='Pedidos criados')
    
    if arquivo:
        df_pedidos = pd.read_csv(os.path.join(PATH, 'downloads/Pedidos criados.csv'), encoding='ISO-8859-1', sep=';', dtype=str)
        pedidos_criados = []

        for _, i in df_pedidos.iterrows():
            pedidos = i['PEDIDOS'].split(', ')
            for pedido in pedidos:
                pedidos_criados.append([pedido, i['PROJETO'].replace('Y-', 'B-')])
        
        df = pd.DataFrame(pedidos_criados, columns=['Pedido', 'Id'])
        df.drop_duplicates(inplace=True, )
        att = GS_SERVICE.sobrescreve_planilha(sh.PEDIDOS, 'PEDIDOS GEOEX', df, input_option='USER_ENTERED')
        print(att)

    else:
        raise('Erro ao baixar o relatório')




if __name__ == '__main__':
    atualiza_pedidos_criados()


with DAG(
    dag_id='atualiza_pedidos_criados',
    tags=['auxiliar'],
    shcedule='*/40 6-22 * * 1-6',
    start_date=pendulum.today('America/Sao_Paulo')
):
    
    atualizacao = PythonOperator(
        task_id='atualiza_pedidos_criados',
        python_callable=atualiza_pedidos_criados
    )


    atualizacao