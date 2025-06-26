from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import json
import numpy as np
import os
import pandas as pd
import pendulum
import re
import sys
from time import sleep


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

from src.geoex import Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.google_sheets import GoogleSheets
GOOGLE_SHEETS = GoogleSheets(credentials='causal_scarab.json') # Inicia o serviço do google sheets

ID_RELATORIOS = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/1VFxxABMX1WQDbYFll2nO5CEp2FGuNpIqTMftncVRpic', aba='id_relatorios_geoex')




def atualizar_base_medicoes():
    #print(os.getcwd())
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
        # ATUALIZA NA PLANILHA GOOGLE
        try:
            #df = pd.read_csv(os.path.join(PATH, 'downloads/Geoex - Relatório - Acompanhamento - Detalhado.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',')
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
            })

            GOOGLE_SHEETS.sobrescreve_planilha(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0', aba='BASE_MEDIÇÕES', df=df_grouped)
                            
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
     

def atualizar_base_hro():
    id_relatorios = []
    id_relatorios.append(ID_RELATORIOS.loc[3].ID)
    id_relatorios.append(ID_RELATORIOS.loc[5].ID)
    df_att = pd.DataFrame()

    for id in id_relatorios:
        download = GEOEX.baixar_relatorio(id)
        if download['sucess']:
            try:
                # Leitura dos dados
                df = pd.read_csv(
                    os.path.join(PATH, 'downloads/Geoex - Processos com HRO - Consulta.csv'),
                    encoding='ISO-8859-1',
                    sep=';',
                    parse_dates=['DT_ENVIO'],
                    dayfirst=True
                )

                # Sequência de tratamento dos dados
                df['PROJETO'] = df['PROJETO'].str.replace('Y-', 'B-') # NUMERO.1 é a antiga coluna PROJETO
                df.query("STATUS != 'CANCELADO'")
                df = df.sort_values('DT_ENVIO', ascending=False)
                df.drop_duplicates('PROCESSO', inplace=True) # NUMERO é a a antiga coluna PROCESSO
                df['DT_ENVIO'] = df['DT_ENVIO'].astype(str)

                print(df)

                # atualizar df_att com os valores de df
                df_att = pd.concat([df_att, df], ignore_index=True)

            except Exception as e:
                raise
        
        else:
            raise Exception(
                f"""
                Falha ao baixar csv.
                Statuscode: { download['status_code'] }
                Message: { download['data'] }
                """
            )
    
    # Atualização da base
    # GOOGLE_SHEETS.sobrescreve_planilha(url='https://docs.google.com/spreadsheets/d/1CcVqctnXFYRIMU4ensbEFqV5LiXJRNu6RflFRBwsJMc', aba='BASE_HRO', df=df_att) # Planilha de testes
    GOOGLE_SHEETS.sobrescreve_planilha(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0', aba='BASE_HRO', df=df_att.fillna(""))


    return {
        'status': 'Ok',
        'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
    }



def atualizar_base_envio_pastas_consulta():
    # Consulta id do relatorio
    id_relatorio = ID_RELATORIOS.loc[1].ID

    download = GEOEX.baixar_relatorio(id_relatorio)

    if download['sucess']:
        try:
            # Leitura dos dados
            df = pd.read_csv(
                os.path.join(PATH, 'downloads/Geoex - Acomp - Envio de pastas - Consulta.csv'),
                encoding='ISO-8859-1',
                sep=';',
                parse_dates=['ENVIO_PASTA_DATA_SOLICITACAO'], 
                dayfirst=True
            )

            # Sequência de tratamento dos dados
            df['PROJETO'] = df['PROJETO'].str.replace('Y-', 'B-')

            data_corte = pd.Timestamp(day=1, month=1, year=2024)
            df = df.query("ENVIO_PASTA_DATA_SOLICITACAO > @data_corte")

            # map_status_ajustado = {
            #     'ACEITO COM RESTRIÇÕES': 'A. Aceita',
            #     'ACEITO': 'A. Aceita',
            #     'VALIDADO': 'B. Validada',
            #     'CRIADO': 'C. Enviada',
            #     'REJEITADO': 'D. Rejeitada',
            #     'CANCELADO': 'E. Cancelada'
            # }
            # df['STATUS AJUSTADO'] = df['STATUS'].map(lambda x: map_status_ajustado[x])

            df = df.sort_values(['ENVIO_PASTA_DATA_SOLICITACAO'], ascending=False)

            df = df.drop_duplicates(subset='PROJETO')

            df['ENVIO_PASTA_DATA_SOLICITACAO'] = pd.to_datetime(df['ENVIO_PASTA_DATA_SOLICITACAO']).dt.strftime('%d/%m/%Y')

            # Atualização da base
            GOOGLE_SHEETS.sobrescreve_planilha(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0', aba='BASE_ENVIO_PASTAS', df=df.fillna("").values.tolist())

        except Exception as e:
            raise

        return {
            'status': 'Ok',
            'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
        }
    
    else:
        raise Exception(
            f"""
            Falha ao baixar csv.
            Statuscode: { download['status_code'] }
            Message: { download['data'] }
            """
        )



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
        controle_materiais = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/1QqO9eRly4n1GPuIvi-oBean1RkSOLl8A0UyL3uS2Lr4', aba='Junção')
        controle_materiais['Quantidade'] = controle_materiais['Quantidade'].replace('', 0)
        controle_materiais = controle_materiais.query("Quantidade != 0 and Código != '' and Projeto.str.startswith('B-') and Tipo == 'MATERIAL'")
        controle_materiais['Quantidade'] = controle_materiais['Quantidade'].astype(float)
        controle_materiais['Código'] = controle_materiais['Código'].astype(int)
        controle_materiais.rename(columns={
            'Código': 'Material',
        }, inplace=True)
        controle_materiais = controle_materiais[['Operação', 'Projeto', 'Material', 'Quantidade']]
        controle_materiais = controle_materiais.groupby(['Operação', 'Projeto', 'Material'], as_index=False).sum()
        
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
        detalhamento_materiais = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/1iX87gud0Q8DJIyntlIxR2UVrnzkRah0TF4CG-_NdryU', aba='Detalhamento dos materiais')
        detalhamento_materiais = detalhamento_materiais[['Material', 'Descrição', 'Categoria']]

        # Faz a junção das bases
        merge = controle_materiais.merge(cji3, on=['Projeto', 'Material'], suffixes=(' Aplicada', ' Movimentada'), how='outer').fillna(0)
        merge = merge.merge(reservas_final1, how='outer', on=['Projeto', 'Material']).rename(columns={'Quantidade': 'Quantidade Disponível (221/921)'}).fillna(0)
        merge = merge.merge(reservas_final2, how='outer', on=['Projeto', 'Material'], suffixes=[' (221/921)', ' (222/922)']).rename(columns={'Quantidade': 'Quantidade Disponível (222/922)'}).fillna(0)
        merge = merge.merge(detalhamento_materiais, on='Material', how='left', validate='many_to_one').fillna("Código não encontrado")

        # Cálculo das quantidades a retirar e a devolver
        qtd_retirar = []
        qtd_devolver = []
        for _, linha in merge.iterrows():
            if linha['Categoria'] == 'SUCATA' or linha['Categoria'] == 'RECUP':
                qtd_retirar.append(0)
                qtd_devolver.append(np.maximum(linha['Quantidade Movimentada'] - linha['Quantidade Aplicada'], 0))
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
            
        merge['Quantidade Retirar'] = pd.Series(qtd_retirar)
        merge['Quantidade Devolver'] = pd.Series(qtd_devolver)


        # Adicionando coluna com os materiais já direcionados na V6
        df_v6 = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/15r7hykDf5EwQ629OB37Z4aTTTfEF_cTs1HExg_qQXE0', aba='Consolidado', dtype=str)
        reservas_em_aberto_v6 = df_v6.query("(Setor == 'MANUT')")[['Reserva']].drop_duplicates()

        materiais_direcionados_v6_retirar = reservas_final1.merge(reservas_em_aberto_v6, left_on='Reserva', right_on='Reserva', how='left')
        materiais_direcionados_v6_retirar = materiais_direcionados_v6_retirar.query("Reserva.notna()").groupby(['Projeto', 'Material'], as_index=False).sum()[['Projeto', 'Material', 'Quantidade']]

        materiais_direcionados_v6_devolver = reservas_final2.merge(reservas_em_aberto_v6, left_on='Reserva', right_on='Reserva', how='left')
        materiais_direcionados_v6_devolver = materiais_direcionados_v6_devolver.query("Reserva.notna()").groupby(['Projeto', 'Material'], as_index=False).sum()
        
        merge = merge.merge(materiais_direcionados_v6_retirar, on=['Projeto', 'Material'], how='left').fillna(0).rename(columns={'Quantidade': 'Quantidade direcionada na V6 (221/921)'})
        merge = merge.merge(materiais_direcionados_v6_devolver, on=['Projeto', 'Material'], how='left').fillna(0).rename(columns={'Quantidade': 'Quantidade direcionada na V6 (222/922)'})


        # Reordena as colunas
        ordem_colunas = ['Projeto', 'Material', 'Descrição', 'Categoria', 'Quantidade Aplicada', 'Quantidade Movimentada', 'Quantidade Disponível (221/921)', 'Quantidade Disponível (222/922)', 'Quantidade Retirar', 'Quantidade Devolver', 'Quantidade direcionada na V6 (221/921)', 'Quantidade direcionada na V6 (222/922)']
        merge = merge[ordem_colunas]

        # Atualiza a base
        GOOGLE_SHEETS.sobrescreve_planilha(url='https://docs.google.com/spreadsheets/d/1iX87gud0Q8DJIyntlIxR2UVrnzkRah0TF4CG-_NdryU', aba='Base', df=merge)

        return {
            'status': 'Ok',
            'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
        }
    
    except Exception as e:
        raise


def criar_hros():
    base = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0', aba='Junção')

    map_operacao_contrato = {
        'GUANAMBI': '4600079167',
        'SANTA MARIA': '4600075605',
        'LAPA': '4600075605',
        'CONQUISTA': '4600079168',
        'IBOTIRAMA': '4600075605',
        'LUIS EDUARDO': '4600075577',
        'BARREIRAS': '4600075577',
        'ITAPETINGA': '4600079168',
        'BRUMADO': '4600079167',
        'JEQUIÉ': '4600079168'
    }

    base_df = pd.DataFrame(base)
    base_df['Contrato'] = base_df['Operação'].map(map_operacao_contrato)
    # criar_hro = base_df.query("`Estágio da pasta`.str.endswith('Pendente criação do HUB (Sirtec - Fechamento)')")
    criar_hro = base_df.query("`Categoria de pagamento` == 'CAPEX_OC' and `Status pasta Geoex` == 'PENDENTE'")

    dados = []
    for _, i in criar_hro.iterrows():
        print(i['OC/PMS'])
        dados.append({
            'Projeto': i['Projeto'],
            'Processo': i['OC/PMS'],
            'Contrato': i['Contrato'],
            'Base': 'CADERNO SD',
            'OC': '',
            'OS': '',
            'PES': '',
            'ContaContrato': '',
            'Observacao': '',
            'Tratamento': 'CRIAR',
        })
    

    json_data = json.dumps(dados)

    files = {
        "file": ("blob", json_data, "text/plain"),
    }


    GEOEX.criar_hro_em_massa(files)


def criar_pastas():
    df = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0', aba='Junção')
    projetos_criar_pasta = df[
        (df["Estágio da pasta"].str.endswith("Pendente postagem da pasta (Sirtec - Fechamento)")) &
        (df["Status pasta Geoex"] != "REJEITADO")
    ]["Projeto"]

    print("Projetos para criação de pasta:")
    print(projetos_criar_pasta)


    for projeto in projetos_criar_pasta:
        GEOEX.enviar_pasta(projeto)
        sleep(1)


def aceitar_hros():
    base_hro = GOOGLE_SHEETS.le_planilha(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0/edit?usp=sharing', aba='BASE_HRO')
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
    'retries' : 2,
    'retry_delay' : pendulum.duration(seconds=5)
}

with DAG(
    'pipeline_manut',
    schedule='*/30 6-22 * * *',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    #default_view="graph",
    max_active_runs = 1,
    tags = ['manut', 'geoex']
):
    

    atualiza_hro = PythonOperator(
                                task_id='atualiza_hro',
                                python_callable=atualizar_base_hro
    )

    atualiza_medicoes = PythonOperator(
                                    task_id='atualiza_medicoes',
                                    python_callable=atualizar_base_medicoes
    )

    atualiza_base_movimentacoes = PythonOperator(
                                task_id='atualiza_base_movimentacoes',
                                python_callable=atualizar_base_movimentacao
                            )
    
    atualiza_pastas = PythonOperator(
                            task_id='atualiza_pastas',
                            python_callable=atualizar_base_envio_pastas_consulta
                            )

    cria_hros = PythonOperator(
                    task_id='cria_hros',
                    python_callable=criar_hros
                )
    

    cria_pastas = PythonOperator(
                    task_id='cria_pastas',
                    python_callable=criar_pastas
                )

    aceita_hros = PythonOperator(
                    task_id='aceita_hros',
                    python_callable=aceitar_hros
                )

    '''confere_arquivos = PythonOperator(
                    task_id='confere_arquivos',
                    python_callable=conferir_arquivos
                )'''



    atualiza_hro >> cria_hros
    atualiza_medicoes >> cria_hros
    atualiza_base_movimentacoes >> cria_hros
    atualiza_pastas >> cria_hros

    atualiza_hro >> cria_pastas
    atualiza_medicoes >> cria_pastas
    atualiza_base_movimentacoes >> cria_pastas
    atualiza_pastas >> cria_pastas

    atualiza_hro >> aceita_hros