from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import numpy as np
import os
import pandas as pd
import pendulum 
import sys


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import src.spreadsheets as sh  # Arquivo contendo link de todas as planilhas

from src.google_sheets import GoogleSheets # Objeto para interagir com as planilhas google
GS_SERVICE = GoogleSheets(credentials='causal_scarab.json')


def read_cji3():
    try:
        os.rename(os.path.join(PATH, 'assets/cji3.XLS'), os.path.join(PATH, 'assets/cji3.csv'))
    except FileExistsError:
        os.remove(os.path.join(PATH, 'assets/cji3.csv'))
        os.rename(os.path.join(PATH, 'assets/cji3.XLS'), os.path.join(PATH, 'assets/cji3.csv'))
    except FileNotFoundError:
        pass

    cji3 = pd.read_csv(
        os.path.join(PATH, 'assets/cji3.csv'),
        sep='\t',
        encoding='ISO-8859-1',
        skiprows=1,
        decimal=',',
        thousands='.',
        dtype={
            5: float,
            6: float,
            8: float,
        },
    )
    cji3 = cji3.loc[cji3['Unnamed: 1'] == '*']


    cji3 = cji3.rename(lambda x: x.replace(' ', ''), axis='columns')
    cji3 = cji3.rename(columns={
                        'Def.proj.': 'Projeto',
                        'Qtd.entr.': 'Quantidade',
                        'Textobrevematerial': 'Descrição'
                    })
    cji3 = cji3[['Projeto', 'Material', 'Quantidade']]
    cji3['Material'] = cji3['Material'].astype(int)
    cji3 = cji3.query("Quantidade != 0")
    

    return cji3    

def read_zmm370():
    try:
        os.rename(os.path.join(PATH, 'assets/zmm370.XLS'), os.path.join(PATH, 'assets/zmm370.csv'))
    except FileExistsError:
        os.remove(os.path.join(PATH, 'assets/zmm370.csv'))
        os.rename(os.path.join(PATH, 'assets/zmm370.XLS'), os.path.join(PATH, 'assets/zmm370.csv'))
    except FileNotFoundError:
        pass

    zmm370 = pd.read_csv(
        os.path.join(PATH, 'assets/zmm370.csv'),
        sep='\t',
        encoding='ISO-8859-1',
        skiprows=1,
        decimal=',',
        thousands='.',
        dtype={
            3: str,
            6: str,
            7: int,
            10: float,
            11: float,
        }
    )
    
    zmm370 = zmm370.rename(lambda x: x.replace(' ', ''), axis='columns')
    zmm370 = zmm370.rename(columns={
                        'QtdPendente': 'Quantidade',
                    })

    zmm370['Projeto'] = zmm370['Projeto'].str.extract(r'(.*?)-(SUPR|MATR)')[0]

    return zmm370


def atualizar_base_movimentacao():
    try:
        # Leitura dos arquivos das bases CJI3 e ZMM370
        cji3 = read_cji3()
        zmm370 = read_zmm370()

        # Leitura da base de controle dos materiais
        controle_materiais = GS_SERVICE.le_planilha(url=sh.MANUT_FECHAMENTO, aba='Junção')
        controle_materiais['Quantidade'] = controle_materiais['Quantidade'].replace('', 0)
        controle_materiais = controle_materiais.query("Quantidade != 0 and Código != '' and Projeto.str.startswith('B-') and Tipo == 'MATERIAL'")
        controle_materiais['Quantidade'] = controle_materiais['Quantidade'].astype(float)
        controle_materiais['Código'] = controle_materiais['Código'].astype(int)
        controle_materiais.rename(columns={
            'Código': 'Material',
        }, inplace=True)
        controle_materiais = controle_materiais[['Projeto', 'Material', 'Quantidade']]
        controle_materiais = controle_materiais.groupby(['Projeto', 'Material'], as_index=False).sum()
        
        # Separação das reservas final 1 e final 2
        reservas_final1 = zmm370.query("RegistroFinal != 'X' and Tipomovimento in ['221', '921']")[['Projeto', 'Material', 'Quantidade', 'Reserva']]
        reservas_final1['Reserva'] = reservas_final1['Reserva'].astype(str)
        reservas_final1 = reservas_final1.groupby(['Projeto', 'Material'], as_index=False).agg({
            'Quantidade': 'sum',
            'Reserva': lambda x: ', '.join(x)
        })

        reservas_final2 = zmm370.query("RegistroFinal != 'X' and Tipomovimento in ['222', '922']")[['Projeto', 'Material', 'Quantidade', 'Reserva']]
        reservas_final2['Reserva'] = reservas_final2['Reserva'].astype(str)
        reservas_final2 = reservas_final2.groupby(['Projeto', 'Material'], as_index=False).agg({
            'Quantidade': 'sum',
            'Reserva': lambda x: ', '.join(x)
        })

        # Leitura da base do detalhamento dos materiais
        detalhamento_materiais = GS_SERVICE.le_planilha(url=sh.MATERIAIS, aba='Materiais')
        detalhamento_materiais = detalhamento_materiais[['Material', 'Descrição', 'Categoria']]

        # Faz a junção das bases
        merge = controle_materiais.merge(cji3, on=['Projeto', 'Material'], suffixes=(' Aplicada', ' Movimentada'), how='outer', validate='one_to_one').fillna(0)
        merge = merge.merge(reservas_final1, how='outer', on=['Projeto', 'Material'], validate='one_to_one').rename(columns={'Quantidade': 'Quantidade Disponível (221/921)'}).fillna(0)
        merge = merge.merge(reservas_final2, how='outer', on=['Projeto', 'Material'], suffixes=[' (221/921)', ' (222/922)'], validate='one_to_one').rename(columns={'Quantidade': 'Quantidade Disponível (222/922)'}).fillna(0)
        merge = merge.merge(detalhamento_materiais, on='Material', how='left', validate='many_to_one').fillna("Código não encontrado")

        # Cálculo das quantidades a retirar e a devolver
        qtd_retirar = []
        qtd_devolver = []
        qtd_estorno_sucata = []

        for _, linha in merge.iterrows():
            if linha['Categoria'] == 'SUCATA' or linha['Categoria'] == 'RECUP':
                qtd_retirar.append(0)
                qtd_devolver.append(np.maximum(linha['Quantidade Movimentada'] - linha['Quantidade Aplicada'], 0))
                if abs(linha['Quantidade Movimentada']) > abs(linha['Quantidade Aplicada']):
                    qtd_estorno_sucata.append(abs(np.minimum(linha['Quantidade Movimentada'] - linha['Quantidade Aplicada'], 0)))
                else:
                    qtd_estorno_sucata.append(0)
            else:
                if linha['Quantidade Aplicada'] > 0:
                    dif_percentual = np.abs((linha['Quantidade Movimentada'] - linha['Quantidade Aplicada'])/linha['Quantidade Aplicada'])
                else:
                    dif_percentual = 1

                if dif_percentual < 0.03:
                    qtd_retirar.append(0)
                    qtd_devolver.append(0)
                else:
                    qtd_retirar.append(np.maximum(linha['Quantidade Aplicada'] - linha['Quantidade Movimentada'], 0))
                    qtd_devolver.append(np.maximum(linha['Quantidade Movimentada'] - linha['Quantidade Aplicada'], 0))

                qtd_estorno_sucata.append(0)
        

        merge['Quantidade Retirar'] = pd.Series(qtd_retirar)
        merge['Quantidade Devolver'] = pd.Series(qtd_devolver)
        merge['Estornar sucata'] = pd.Series(qtd_estorno_sucata)
        
        merge['Criar reserva de retirada'] = merge.apply(lambda x: np.maximum(x['Quantidade Retirar'] - x['Quantidade Disponível (221/921)'], 0), axis=1)
        merge['Criar reserva de devolução'] = merge.apply(lambda x: np.maximum(x['Quantidade Devolver'] - x['Quantidade Disponível (222/922)'], 0), axis=1)
        merge['Eliminar reserva lixo'] = merge.apply(lambda x: np.maximum(x['Quantidade Disponível (221/921)'] - x['Quantidade Retirar'],0) + np.maximum(x['Quantidade Disponível (222/922)'] - x['Quantidade Devolver'],0), axis=1)
        


        ### Define status das movimentações

        # Define os projetos com nenhum material aplicado como pendente de orçamento
        soma_aplicada = merge.groupby('Projeto')['Quantidade Aplicada'].apply(lambda x: x.abs().sum())
        merge['Status movimentação'] = merge['Projeto'].map(
                lambda projeto: 'A. Orçamento pendente' if soma_aplicada.get(projeto, 0) <= 0 else None
            )
        
        # Define os materiais pendentes de criação de reserva
        merge.loc[(((merge['Criar reserva de devolução'] + merge['Criar reserva de retirada']) > 0)) & merge['Status movimentação'].isnull(), 'Status movimentação'] = 'B. Pendente criação de reservas'

        # Define os materiais pendentes de direcionamento na V6
        

        # Define materiais pendentes de movimentação
        merge.loc[((merge['Quantidade Retirar'] > 0) | (merge['Quantidade Devolver'] > 0)) & merge['Status movimentação'].isnull(), 'Status movimentação'] = 'D. Pendente movimentação'

        # Define sucatas pendentes de estorno
        merge.loc[ ((merge['Categoria'] == 'SUCATA') | (merge['Categoria'] == 'RECUP')) & merge['Status movimentação'].isnull() & ( merge['Estornar sucata'] > 0 ), 'Status movimentação'] = 'E. Pendente de estornar sucata'

        # Define reservas lixo
        # merge.loc[merge['Status movimentação'].isnull() & (merge['Eliminar reserva lixo'] > 0) & ((merge['Quantidade Retirar'] == 0) & (merge['Quantidade Devolver'] == 0) ), 'Status movimentação'] = 'F. Pendente eliminar reservas lixo'

        # Define as movimentações Ok
        merge.loc[merge['Status movimentação'].isnull(), 'Status movimentação'] = 'G. Movimentação ok'

        projetos_postagem = GS_SERVICE.le_planilha(sh.MANUT_POSTAGEM, aba='Ocorrências', intervalo='A:F')
        projetos_postagem = projetos_postagem.groupby('Projeto')[['Projeto', 'UTD', 'Categoria de pagamento']].agg({'UTD':'first', 'Categoria de pagamento': 'first'})
        merge = merge.merge(projetos_postagem, on='Projeto', how='left')
        

        ### Atualiza a base
        
        # Reordenando as colunas
        merge.sort_values(by=['Projeto', 'Status movimentação'], ascending=[True, True], inplace=True)
        ordem_colunas = ['Categoria de pagamento', 'UTD', 'Projeto', 'Material', 'Descrição', 'Status movimentação', 'Quantidade Aplicada', 'Quantidade Movimentada', 'Quantidade Disponível (221/921)', 'Quantidade Disponível (222/922)', 'Quantidade Retirar', 'Quantidade Devolver',  'Criar reserva de retirada', 'Criar reserva de devolução', 'Eliminar reserva lixo', 'Estornar sucata', 'Reserva (221/921)', 'Reserva (222/922)']
        merge = merge[ordem_colunas].fillna("")
        
        # Atualiza a base
        sucess = GS_SERVICE.sobrescreve_planilha(url=sh.MANUT_POSTAGEM, aba='BASE_MOVIMENTAÇÕES', df=merge)
        if sucess:
            GS_SERVICE.escreve_planilha(url=sh.MANUT_POSTAGEM, aba='Atualizações', df=pd.DataFrame([['Movimentação de materiais', datetime.now().strftime("%d/%m/%Y, %H:%M")]]), range='A2')

            return {
                'status': 'Ok',
                'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
            }
        else:
            return{
                'status': 'Fail',
                'message': 'Falha na atualização!'
            }
    
    except Exception as e:
        raise



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
    dag_id='atualizar_mov_materiais',
    tags=['manut'],
    schedule='*/30 6-22 * * *',
    default_args=default_args,
    start_date=pendulum.today('America/Sao_Paulo')
):

    atualizar_mov_materiais = PythonOperator(
        task_id='atualizar_mov_materiais',
        python_callable=atualizar_base_movimentacao,

    )

    atualizar_mov_materiais