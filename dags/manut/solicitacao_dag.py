#airflow
#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
#bibliotecas
'''import os
import sys'''
import spreadsheets
import pandas as pd
from time import sleep
from pendulum import today, duration
#classes próprias
from src.geoex import Geoex
from src.google_sheets import GoogleSheets

'''PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)'''

GEOEX = Geoex(cookie_file='cookie_hugo.json')
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json') # Inicia o serviço do google sheets

endpoint_reserva = 'ConsultarProjetoSolicitacaoReserva/Itens'
reservas_ids = {
    1: 'CRIADO',
    6 : 'CANCELADO',
    30 : 'ACEITO',
    31 : 'ACEITO COM RESTRIÇÕES',
    32 : 'REJEITADO',
    35 : 'VALIDADO',
    148 : 'ATENDIDO'
}

def consulta_solicitacao(projeto):
    status = ['']
    serial = ['']
    reserva = [''] 

    r = GEOEX.consultar_projeto(projeto)
    if r['sucess']:
        projeto_id = r['data']['ProjetoId']
    elif r['status_code'] == 400:
        print('Projeto sem contrato')
        return status, serial, reserva
    else:
        raise Exception(
            f"""
            Falha consultar projeto.
            Statuscode: { r['status_code'] }
            Message: { r['data'] }
            """
        ) 

        
    try:
        body = {'ProjetoId': projeto_id,
                'Rejeitadas': False,
                'Paginacao': {'TotalPorPagina': "10", 'Pagina': "1"}}
    except Exception as e:
        print(projeto, r['Message'], r['StatusCode'], r['Content'])
        raise e
        
    sleep(1)

    try:
        r = GEOEX.run("POST", endpoint_reserva, json=body).json()
    except Exception as e:
        print(projeto, r.content, str(r.status_code), str(r.reason))
        raise e
    
    try:
        if r['Content']['TotalWithFilter']!=0:
            status = []
            serial = []
            reserva = []
            for i in r['Content']['Items']:
                id = i['HistoricoAtual']['HistoricoStatusId']
                serial.append(i['Serial'])
                reserva.append(i['NumeroReserva'])
                status.append(reservas_ids.get(id, id))
    except Exception as e:
        if r['Message']=='Não foi possível processar sua solicitação. Ocorreu um erro no servidor. Tente novamente.':
            status = ['ERRO']
            serial = ['']
        else:
            print(projeto, r['Message'], r['Content'])
            raise e
            
    return status, serial, reserva
    
def atualiza_solicitacoes():
    titulo = ['Projeto','Status','Solicitação','Reserva']
    sheet = GS_SERVICE.le_planilha(url=spreadsheets.SOLICITACOES_RESERVA, aba='Junção')
    sheet['Projeto'] = sheet['Projeto'].str.strip()
    sheet = sheet[sheet['Projeto']!='']
    sheet = sheet[sheet['Categoria de pagamento'].isin(['CAPEX_OC', 'CAPEX_PREV'])]
    sheet = sheet[sheet['Status movimentação dos materiais'].isin(['B. Pendente criação de reservas'])]
    sheet = sheet.sort_values(by=['Projeto','Status movimentação dos materiais'], ascending=[True, True])
    sheet = sheet.drop_duplicates(subset=['Projeto'], keep='first')
    
    valores = []
    for i, projeto in enumerate(sheet['Projeto']):
        status, serial, reserva = consulta_solicitacao(projeto)
        for j, k, l in zip(status, serial, reserva):
            valores.append([projeto, j, k, l])
        print(f'{i+1}/{sheet.shape[0]}: {projeto}, {status}, {serial}, {reserva}')
    

    aba = 'Solicitações'
    df = pd.DataFrame(valores, columns=titulo)
    GS_SERVICE.limpa_intervalo(spreadsheets.SOLICITACOES_RESERVA, aba, 'A2:D')
    GS_SERVICE.escreve_planilha(spreadsheets.SOLICITACOES_RESERVA, aba, df=df, range='A2:D', input_option='USER_ENTERED')




default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('solicitacoes-de-reservas',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0 6,12 * * 1-6',
        max_active_runs = 1,
        tags = ['reservas', 'geoex', 'manut'],
        catchup = False) as dag:

    solicitacoes = PythonOperator(
        task_id = 'solicitacoes',
        python_callable = atualiza_solicitacoes
    )

    solicitacoes