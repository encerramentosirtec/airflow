#airflow
#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
#bibliotecas
import pandas as pd
from time import sleep
from datetime import datetime
from pendulum import duration, today, timezone
#classes próprias
from src.geoex import Geoex
from src.bots_ccm import Bots
from src.config import configs
from src.google_sheets import GoogleSheets

unidades = ['BRUMADO', 'CONQUISTA', 'BARREIRAS', 'IRECE', 'JEQUIE', 'IBOTIRAMA', 'LAPA', 'GUANAMBI']
BOT = Bots('cookie_ccm.json', 'jimmy.json')
GEOEX = Geoex(cookie_file='cookie_ccm.json')
GS_SERVICE = GoogleSheets(credentials='jimmy.json')

def pesquisa_geoex(projeto, atesto):
    body = {
        'id': projeto
    }

    try:
        r = BOT.fazer_requisicao(BOT.url_geo, body)
    except Exception as e:
        print('Não foi possível acessar a página do Geoex.')
        print(e)
        return 'erro'
    
    if r['Content'] != None:
        id_projeto = r['Content']['ProjetoId']

        url = 'ConsultarProjeto/Postagem/Itens'
        body = {
            "ProjetoId": id_projeto,
            "Search": atesto,
            "Rejeitadas": True,
            "Arquivadas": False,
            "Paginacao": {
            "TotalPorPagina": "10",
            "Pagina": "1"
            }
        }

        r = BOT.fazer_requisicao(url, body)
    
        if r['Content']['Items'] == []:
            print('ID',atesto,'não pertence ao projeto',projeto + '.')
            return 'erro'
        else:
            status_id = r['Content']['Items'][0]['Status']

            if status_id == "Rejeitado":
                status_id = "Rejeitado"
            elif status_id == "Postado":
                status_id = atesto
            elif status_id != "Rejeitado":
                if status_id != "Postado":
                    status_id = "OK"
            else:
                status_id = status_id
            
            return status_id
    elif r['IsUnauthorized']:
        print(r)
        raise Exception('Cookie inválido! Não autorizado')
    else:
        print(r)
        raise Exception('Erro não identificado')

def atualiza_aba_v2(aba):
    while True:
        try:
            v5 = GS_SERVICE.le_planilha(configs.id_planilha_postagemV5, aba)
            break
        except Exception as e:
            print(e)
            sleep(30)

    projetos = v5['PROJETO']
    colunas = ['ATESTO CAVA', 'ATESTO SUCATA', 'ATESTO LINHA VIVA', 'ATESTO EXCEDENTE', 'ATESTO FUNDAÇÃO', 'ARRASTAMENTO', 'ABERTURA DE FAIXA', 'ATESTO PODA']
    status_ids = [[] for _ in range(len(colunas))]

    for i, coluna in enumerate(colunas):
        sleep(2)
        atestos = v5[coluna]
        for projeto, atesto in zip(projetos, atestos):
            if str(atesto).startswith('PM'):
                status = pesquisa_geoex(projeto, atesto)
                if status != 'erro':
                    status_ids[i].append([status])
                else:
                    status_ids[i].append([atesto])
            else:
                status_ids[i].append([atesto])
                status = atesto
            
            print(f'{coluna} {projetos[projetos == projeto].index.tolist()[0]+1}/{projetos.shape[0]} - projeto: {projeto}, atesto: {atesto}, status: {status}')

    for i, coluna in enumerate(colunas):
        v5[coluna] = status_ids[i]

    intervalos = ['U2:U','V2:V','W2:W','Y2:Y','Z2:Z','AA2:AA','AB2:AB','AE2:AE']
    while True:
        try:
            df = pd.DataFrame(status_ids).transpose()
            df.columns = colunas
            for i,j in zip(colunas,intervalos):
                GS_SERVICE.escreve_planilha(configs.id_planilha_postagemV5, aba, df[i], j, 'USER_ENTERED')
            break
        except Exception as e:
            print(e)
            sleep(30)

def atualiza_data_v2():
    spreadsheetId = '18-AoLupeaUIOdkW89o6SLK6Z9d8X0dKXgdjft_daMBk'
    range_name = 'D2:D2'
    aba = 'Atestos pendentes'
    br_tz = timezone("Brazil/East")
    hora = datetime.now(br_tz).strftime("%d/%m/%Y %H:%M")

    while True:
        try:
            df = pd.DataFrame([hora], columns=['hora'])
            GS_SERVICE.escreve_planilha(spreadsheetId, aba, df, range_name, 'USER_ENTERED')
            print(f"Data atualizada.")
            break
        except Exception as err:
            print(err)
            sleep(30)

    print("Status dos atesto atualizados!\n")


default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'bob',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('v2_atesto',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    tarefas = []
    for nome in unidades:
        task = PythonOperator(
            task_id=f"{nome.lower()}",
            python_callable=atualiza_aba_v2,
            op_args=[f'OBRAS {nome}']
        )
        tarefas.append(task)
    
    atualiza_data = PythonOperator(
        task_id='atualiza_data',
        python_callable=atualiza_data_v2
    )
    
    for prev, next_ in zip(tarefas, tarefas[1:]):
        prev >> next_
    tarefas[-1]>>atualiza_data