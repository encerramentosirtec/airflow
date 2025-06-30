#airflow
from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
#bibliotecas
import traceback
import pandas as pd
from time import sleep
from datetime import datetime
from pendulum import timezone, duration, today
#classes próprias
from src.geoex import Geoex
from src.config import configs
from src.google_sheets import GoogleSheets

GEOEX = Geoex(cookie_file='cookie_ccm.json')
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json')

def consulta_projeto(projeto):
        datazps09 = ''
        projeto = str(projeto)

        r = GEOEX.consultar_projeto(projeto)

        if r['sucess']:
            if r['Content']['DtZps09'] != None:
                datazps09 = datetime.fromisoformat(r['data']['DtZps09']).date().strftime("%d/%m/%Y")
        else:
            raise Exception(
                f"""
                Falha consurtar projeto.
                Statuscode: { r['status_code'] }
                Message: { r['data'] }
                """
            )
        
        return datazps09

def atualiza_zps09():
        aba = 'Auxiliar (transporte)'

        while True:
            try:
                sheet = GS_SERVICE.le_planilha(configs.id_planilha_postagemV5, aba)
                break
            except Exception as e:
                print(e)

        valores = [[],[],[],[]]

        print('Atualizando ZPS09')

        tamanho = sheet.shape[0]
        
        for i,j in enumerate(sheet['PROJETO']):
            if j=="":
                valores.append([''])
                zps09 = ''
            else:
                if sheet['DATA ZPS09'][i]!='':
                    valores.append([])
                    zps09=f'{sheet['DATA ZPS09'][i]} (existente)'
                else:
                    try:
                        zps09 = consulta_projeto(j)
                        valores.append([zps09])
                        sleep(1)
                    except Exception as e:
                        print(f'Erro no projeto {j}')
                        traceback.print_exc()
                        raise e
            
            print(f'{str(i+1)}/{str(tamanho)} - {j}: {zps09}')

        while True:
            try:
                df = pd.DataFrame(valores, columns=['ZPS09'])
                GS_SERVICE.escreve_planilha(configs.id_planilha_postagemV5, aba, df, 'E2', 'USER_ENTERED')
                break
            except Exception as e:
                print(e)

        print('ZPS09 atualizada!')

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'retries' : 2,
    'owner' : 'bob'
}

with DAG('atualizarzps09',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0 8,11,13,15 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    atualizarzps09 = PythonOperator(
        task_id='atualizarzps09',
        python_callable=atualiza_zps09,
        retries=2,
        retry_delay=duration(seconds=20)
    )

    atualizarzps09