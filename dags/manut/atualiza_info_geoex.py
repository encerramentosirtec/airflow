#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
from datetime import datetime
import os
import pendulum
from time import sleep
import sys

PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh


from src.geoex import Geoex
GEOEX = Geoex('cookie_hugo.json')

from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('causal_scarab.json')


def criar_linha_da_tabela_pastas_geoex(projeto):
    linha = list()
    infos_projeto = GEOEX.consultar_projeto(projeto)
    print(infos_projeto)

    map_operacao_contrato = {
        'GUANAMBI': '4600079167',
        'SANTA MARIA DA VITÓRIA': '4600075605',
        'BOM JESUS DA LAPA': '4600075605',
        'VITÓRIA DA CONQUISTA': '4600079168',
        'IBOTIRAMA': '4600075605',
        'LUIS EDUARDO MAGALHÃES': '4600075577',
        'BARREIRAS': '4600075577',
        'ITAPETINGA': '4600079168',
        'BRUMADO': '4600079167',
        'JEQUIÉ': '4600079168'
    }


    if infos_projeto['sucess']:
        projeto_id = infos_projeto['data']['ProjetoId']

        linha.append(projeto) # Coluna PROJETO
        linha.append(infos_projeto['data']['Titulo']) # Coluna TITULO
        linha.append(infos_projeto['data']['VlProjeto']) # Coluna VALOR PROJETO
        linha.append(infos_projeto['data']['ValorServico']) # Coluna VALOR SERVICO
        linha.append(infos_projeto['data']['ValorMaterial'] )# Coluna VALOR MATERIAL
        linha.append(infos_projeto['data']['StatusProjeto']['Descricao']) # Coluna STATUS DO PROJETO
        linha.append(infos_projeto['data']['StatusUsuario']['Descricao']) # Coluna STATUS USUARIO - OPERAÇÃO
    
        if infos_projeto['data']['StatusFlutuante']:
            linha.append(infos_projeto['data']['StatusFlutuante']['Nome']) # Coluna STATUS FLUTUANTE - ENCERRAMENTO
        else:
            linha.append('')

        if infos_projeto['data']['GseProjeto']:
            linha.append(infos_projeto['data']['GseProjeto']['Serial']) # Coluna PROJETO GSE
            linha.append(infos_projeto['data']['GseProjeto']['Status']['Nome']) # Coluna STATUS HEKTOR
        else:
            linha.append('')
            linha.append('')
        
        situacao_arquivos = GEOEX.consultar_arquivos(projeto_id)
        linha.append(situacao_arquivos[0]) # ARQUIVOS 015
        linha.append(situacao_arquivos[1]) # ARQUIVOS 018
        linha.append(situacao_arquivos[2]) # ARQUIVOS 021
        linha.append(situacao_arquivos[3]) # ARQUIVOS 127
        linha.append(situacao_arquivos[4]) # ARQUIVOS 022

        linha.append(infos_projeto['data']['Localidade']) # LOCALIDADE
        linha.append(infos_projeto['data']['Municipio']) # MUNICÍPIO

        linha.append(infos_projeto['data']['Empresa'])
        linha.append(infos_projeto['data']['ProjetoText'])

        data_energ = infos_projeto['data']['DtZps09'] # DATA ENERGIZAÇÃO
        if data_energ == None:
            linha.append('')
        else:
            data_energ = pd.to_datetime(data_energ)
            data_energ = data_energ.strftime('%d/%m/%Y')
            linha.append(data_energ) # DATA ENERGIZAÇÃO

        if infos_projeto['data']['ProjetoGrupoItem']: # GRUPO
            linha.append(infos_projeto['data']['ProjetoGrupoItem']['Nome'])
        else:
            linha.append('')

        linha.append(infos_projeto['data']['Unidade'])
        linha.append(infos_projeto['data']['PosicaoInvestimentoIdExterno'])

        if infos_projeto['data']['UsuarioResponsavel']:
            linha.append(infos_projeto['data']['UsuarioResponsavel']['Nome'])
        else:
            linha.append('')

        return {'status_code': infos_projeto['status_code'], 'data': linha}

    else:
        linha.append(projeto)
        linha.append(infos_projeto['data'])
        linha = ['' if x is None else x for x in linha] # Subsitui por espaço em branco os valores nulos da lista

        return {'status_code': infos_projeto['status_code'], 'data': linha}



def atualiza_base():
    try:
        planilha_online = GS_SERVICE.le_planilha(sh.MANUT_POSTAGEM, aba='Junção')
        projetos = planilha_online['Projeto'].unique()

        tabela = list()
        for projeto in projetos:
            while True:
                linha = criar_linha_da_tabela_pastas_geoex(projeto)
                print(projeto, linha)
                if linha['status_code'] == 200:
                    tabela.append(linha['data'])
                    break
                elif linha['status_code'] == 429:
                    sleep(60)
                    continue
                elif linha['status_code'] == 403:
                    raise Exception("Status 403: Atualizar autenticador.")
                else:
                    tabela.append(linha['data'])
                    break
            
            sleep(1.5)

        colunas = [
                'Projeto', 
                'Título', 
                'Valor Projeto', 
                'Valor Serviço', 
                'Valor Material', 
                'Status do projeto', 
                'Status usuário - Operação', 
                'Status flutuante - Encerramento', 
                'Projeto GSE', 
                'Status Héktor', 
                'Arquivos 015', 
                'Arquivos 018', 
                'Arquivos 021', 
                'Arquivos 127', 
                'Arquivos 022', 
                'Localidade', 
                'Município', 
                'Empresa', 
                'ProjetoText', 
                'Data ZPS09', 
                'Grupo', 
                'Unidade', 
                'Id Investimento', 
                'Responsável'
            ]

        df = pd.DataFrame(tabela, columns=colunas).fillna("")
        sucess = GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_GEOEX', df=df, input_option='USER_ENTERED')
        if sucess:
            GS_SERVICE.escreve_planilha(url=sh.MANUT_POSTAGEM, aba='Atualizações', df=pd.DataFrame([['Projetos Geoex', datetime.now().strftime("%d/%m/%Y, %H:%M")]]), range='A6')

        print( {
            'status': 'Ok',
            'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Informações das pastas atualizadas!"
        }    )

    except Exception as e:
        raise


if __name__ == '__main__':
    atualiza_base()
    sys.exit()

default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 2,
    'retry_delay' : pendulum.duration(seconds=5)
}

with DAG(
    'atualiza_base_geoex_manut',
    schedule = '0 6,12 * * 1-6',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    #default_view="graph",
    max_active_runs = 1,
    tags = ['manut', 'geoex']
):
    
    atualiza_geoex = PythonOperator(
        task_id='atualiza_geoex_manut',
        python_callable = atualiza_base
    )


    atualiza_geoex