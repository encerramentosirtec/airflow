from datetime import datetime
import gspread
import json
import numpy as np
import os
import pandas as pd
from time import sleep
import re

PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código
GS_SERVICE = gspread.service_account(filename=os.path.join(PATH, 'assets/auth_google/causal_scarab.json')) # Inicia o serviço do google sheets


from src.geoex import Geoex

class Bots:

    def __init__(self):
        self.geoex = Geoex(json_file='cookie_hugo.json')


    def le_planilha_google(self, url, aba, render_option='UNFORMATTED_VALUE'):
        sh = GS_SERVICE.open_by_url(url)
        ws = sh.worksheet(aba)
        df = pd.DataFrame(ws.get_all_records(value_render_option=render_option))

        return df


    def atualizar_base_medicoes(self):
        #print(os.getcwd())
        map_status = {
            'MPC': 'A. Pedido lançado',
            'MVD': 'B. Validada',
            'MEA': 'C. Atestada',
            'MPA': 'D. Postada',
            'MRJ': 'E. Rejeitada'
        }

        ids = self.le_planilha_google(url='https://docs.google.com/spreadsheets/d/1VFxxABMX1WQDbYFll2nO5CEp2FGuNpIqTMftncVRpic/edit?gid=0#gid=0', aba='id_relatorios_geoex')
        id_relatorio = ids.loc[0].ID
        download = self.geoex.baixar_relatorio(id_relatorio)
        if download['sucess']:
            # ATUALIZA NA PLANILHA GOOGLE
            try:
                #df = pd.read_csv(os.path.join(PATH, 'downloads/Geoex - Relatório - Acompanhamento - Detalhado.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',')
                df = pd.read_csv(os.path.join(PATH, 'downloads/Geoex - Relatório - Acompanhamento - Detalhado.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',')
                
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


                sh = GS_SERVICE.open_by_url('https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0/edit?gid=1657459809#gid=1657459809')
                ws = sh.worksheet('BASE_MEDIÇÕES')

                ws.clear()
                ws.update(range_name='A1', values=[df_grouped.columns.tolist()])
                ws.update(range_name='A2', values=df_grouped.fillna("").values.tolist())
                                
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
        

    def atualizar_base_envio_pastas_consulta(self):
        print('sinal1')
        # Consulta id do relatorio
        sh = GS_SERVICE.open_by_key('1VFxxABMX1WQDbYFll2nO5CEp2FGuNpIqTMftncVRpic')
        ws = sh.worksheet("id_relatorios_geoex")
        ids = pd.DataFrame(ws.get_all_records())
        id_relatorio = ids.loc[1].ID

        download = self.geoex.baixar_relatorio(id_relatorio)

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
                sh = GS_SERVICE.open_by_key('1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0')
                ws = sh.worksheet('BASE_ENVIO_PASTAS')
                ws.clear()
                ws.update(range_name='A1', values=[df.columns.tolist()])
                ws.update(range_name='A2', values=df.fillna("").values.tolist())
            
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
        

    def atualizar_base_hro(self):
        sh = GS_SERVICE.open_by_key('1VFxxABMX1WQDbYFll2nO5CEp2FGuNpIqTMftncVRpic')
        ws = sh.worksheet("id_relatorios_geoex")
        ids = pd.DataFrame(ws.get_all_records())

        id_relatorios = []
        id_relatorios.append(ids.loc[3].ID)
        id_relatorios.append(ids.loc[5].ID)
        df_att = pd.DataFrame()

        for id in id_relatorios:
            download = self.geoex.baixar_relatorio(id)
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
        sh = GS_SERVICE.open_by_key('1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0')
        # sh = GS_SERVICE.open_by_key('1CcVqctnXFYRIMU4ensbEFqV5LiXJRNu6RflFRBwsJMc') # Planilha de testes
        ws = sh.worksheet('BASE_HRO')
        ws.clear()
        ws.update(range_name='A1', values=[df_att.columns.tolist()])
        ws.update(range_name='A2', values=df_att.fillna("").values.tolist())

        return {
            'status': 'Ok',
            'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
        }



    def read_cji3(self):
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

    def read_zmm370(self):
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


    def atualizar_base_movimentacao(self):
        try:
            # Leitura dos arquivos das bases CJI3 e ZMM370
            cji3 = self.read_cji3()
            zmm370 = self.read_zmm370()

            # Leitura da base de controle dos materiais
            sh = GS_SERVICE.open_by_key('1QqO9eRly4n1GPuIvi-oBean1RkSOLl8A0UyL3uS2Lr4')
            ws = sh.worksheet('Junção')
            controle_materiais = pd.DataFrame(ws.get_all_records(value_render_option='UNFORMATTED_VALUE'))
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
            sh = GS_SERVICE.open_by_key('1iX87gud0Q8DJIyntlIxR2UVrnzkRah0TF4CG-_NdryU')
            ws = sh.worksheet('Detalhamento dos materiais')
            detalhamento_materiais = pd.DataFrame(ws.get_all_records())
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
            sh = GS_SERVICE.open_by_url(r'https://docs.google.com/spreadsheets/d/15r7hykDf5EwQ629OB37Z4aTTTfEF_cTs1HExg_qQXE0')
            ws = sh.worksheet('Consolidado')

            df_v6 = pd.DataFrame(ws.get_all_records(value_render_option='UNFORMATTED_VALUE'), dtype='str')
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
            sh = GS_SERVICE.open_by_key('1iX87gud0Q8DJIyntlIxR2UVrnzkRah0TF4CG-_NdryU')
            ws = sh.worksheet('Base')
            ws.update(range_name='A2:J', values=[[""]*10]*25000)
            ws.update(range_name='A2', values=merge.values.tolist())

            return {
                'status': 'Ok',
                'message': f"[{  datetime.strftime(datetime.now(), format='%H:%M')  }] Base atualizada!"
            }
        
        except Exception as e:
            raise


    def criar_hros(self):
        ws = GS_SERVICE.open_by_url(r'https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0')
        sh = ws.worksheet('Junção')

        base = sh.get_all_records(value_render_option='UNFORMATTED_VALUE')

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


        self.geoex.criar_hro_em_massa(files)


    def criar_pastas(self):
        ws = GS_SERVICE.open_by_url('https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0/edit?gid=1852988126#gid=1852988126')
        sh = ws.worksheet('Junção')
        df = pd.DataFrame(sh.get_all_records(value_render_option='UNFORMATTED_VALUE'))

        projetos_criar_pasta = df[
            (df["Estágio da pasta"].str.endswith("Pendente postagem da pasta (Sirtec - Fechamento)")) &
            (df["Status pasta Geoex"] != "REJEITADO")
        ]["Projeto"]

        print("Projetos para criação de pasta:")
        print(projetos_criar_pasta)


        for projeto in projetos_criar_pasta:
            self.geoex.enviar_pasta(projeto)
            sleep(1)


    def aceitar_hros(self):
        base_hro = self.le_planilha_google(url='https://docs.google.com/spreadsheets/d/1wb7jj5wQM_61-yQruIn4UGI9KrQH4CL__W-X0MPcif0/edit?usp=sharing', aba='BASE_HRO')
        hros = base_hro.query("STATUS == 'ANALISADO'")['PROTOCOLO']
        for hro in hros:
            r = self.geoex.aceitar_hro(hro)
            print(hro)
            print(r['data'])
            