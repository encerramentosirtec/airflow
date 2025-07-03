#airflow
#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
#bibliotecas
import pandas as pd
from time import sleep
from pendulum import duration, today
#classes próprias
from src.bots_ccm import Bots
from src.config import configs
from src.google_sheets import GoogleSheets

BOT = Bots('cookie_ccm.json', 'global_brook.json')
GS_SERVICE = GoogleSheets(credentials='global_brook.json')

def lv_geral():
    aba = 'LV GERAL'
    while True:
        try:
            lv = GS_SERVICE.le_planilha(configs.id_planilha_postagemV5, aba, 'B:B')
            break
        except Exception as e:
            print(e)
            sleep(5)

    lv = lv[lv['PROJETO']!='']
    lv['PROJETO'] = lv['PROJETO'].str.strip()

    status = []
    for index, row in lv.iterrows():
        primeiro = lv[lv['PROJETO'] == row['PROJETO']].index[0]
        
        if row['PROJETO'] and row['PROJETO']!='' and primeiro==index:
            data = {"id": row['PROJETO']}
            
            try:
                resposta = BOT.fazer_requisicao(BOT.url_geo, data)
                id_projeto = resposta['Content']['ProjetoId']
                sleep(1)
                
                body_pasta = {'ProjetoId': id_projeto} 
                
                try:
                    resposta_pasta = BOT.fazer_requisicao(url=BOT.url_pasta, body=body_pasta)#, headers=headers)
                    conteudo_pasta = resposta_pasta['Content']['Envios']
                    sleep(1)
                    status_pasta = ''
                    for i in conteudo_pasta:
                        if i['EmpresaId']==70:
                            status_pasta = BOT.statuspastaid.get(i['Ultimo']['HistoricoStatusId'],i['Ultimo']['HistoricoStatusId'])
                            break
                        else:
                            status_pasta = ''    
                except:
                    status_pasta = ''
                
                status.append(str(status_pasta))
            except Exception as e:
                if str(e) == 'Cookie Inválido':
                    raise e
                print(f'erro {e}')
                status_pasta = ''
                status.append('')
                pass
        elif row['PROJETO']!='' and primeiro!=index and primeiro<len(status):
            #print(primeiro, index, row['PROJETO'], len(status))
            status.append(str(status[primeiro]))
            print(f'{(index+1):03}/{lv.shape[0]} - projeto: {row['PROJETO']}, status pasta: {status[primeiro]} (repetido)')
            continue
        else:
            status.append('')
        
        print(f'{(index+1):03}/{lv.shape[0]} - projeto: {row['PROJETO']}, status pasta: {status_pasta}')

    while True:
        try:
            df_dict = pd.DataFrame(status)
            GS_SERVICE.escreve_planilha(configs.id_planilha_postagemV5, aba, df_dict, 'Z2', 'USER_ENTERED')
            break
        except Exception as e:
            print(e)
            sleep(30)

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'bob',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('lv',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    lv = PythonOperator(
        task_id='lv',
        python_callable=lv_geral
    )

    lv