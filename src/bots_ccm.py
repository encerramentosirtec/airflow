import os
import re
import json
import gspread
import numpy as np
import pandas as pd
from time import sleep
from src.geoex import Geoex
from pendulum import timezone
from datetime import datetime
from src.config import configs
from hooks.geoex_hook import GeoexHook

class Bots:

    def __init__(self, cookie_path='assets/auth_geoex/cookie_ccm.json', cred_path='assets/auth_geoex/causal_scarab.json'):
        self.PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código
        
        with open(os.path.join(self.PATH, cookie_path), 'r') as f:
            self.cookie = json.load(f)
            
        self.geoex = Geoex(cookie_path)
        self.GS_SERVICE = gspread.service_account(filename=os.path.join(os.getcwd(), cred_path))
        self.br_tz = timezone("Brazil/East")



    def le_planilha_google(self, url, aba, intervalo=None, render_option='UNFORMATTED_VALUE'):
        try:
            sh = self.GS_SERVICE.open_by_url(url)
        except:
            sh = self.GS_SERVICE.open_by_key(url)
        ws = sh.worksheet(aba)
        if intervalo == None:
            df = pd.DataFrame(ws.get_all_values(value_render_option=render_option))
        else:
            df = pd.DataFrame(ws.get_all_values(range_name='A1:F', value_render_option=render_option))

        return df


    def asbuilt(self):
        ####################### LENDO CARTEIRAS ONLINE
        while True:
            try:
                carteira_g = self.le_planilha_google(configs.carteira_g, "Página1", 'A1:F')
                carteira_g = carteira_g[['Dt. En. GEOEX', 'Projeto', 'Status Execução', 'Unidade', 'Supervisor', 'Município']]
                carteira_g['Projeto'] = carteira_g['Projeto'].str.replace('B-', '')
                carteira_g.columns = ['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'MUNICÍPIO']
                carteira_g = carteira_g.query("PROJETO != ''")
                carteira_g = carteira_g[carteira_g['STATUS GERAL'].isin(['CONCLUÍDA', 'Concluída', 'CONCLUIDA', '-CONCLUIDA'])]
                print('lendo carteira_g')

                carteira_barreiras = self.le_planilha_google(configs.id_planilha_planejamento, "Carteira_Resumida", 'E10:CL')
                carteira_barreiras = carteira_barreiras[['Ult. Carteira', 'Projeto', 'Status Execução', 'Unidade', 'Supervisor', 'Município']]
                carteira_barreiras['Projeto'] = carteira_barreiras['Projeto'].str.replace('B-', '')
                carteira_barreiras = carteira_barreiras.query("Projeto != ''")
                carteira_barreiras.columns = ['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'MUNICÍPIO']
                carteira_barreiras = carteira_barreiras[carteira_barreiras['STATUS GERAL'].isin(['CONCLUÍDA', 'Concluída', 'CONCLUIDA', '-CONCLUIDA'])]
                print('lendo carteira_barreiras')
                
                carteira_CCM = self.le_planilha_google(configs.carteira_CCM, "Carteira_Completa")
                carteira_CCM = carteira_CCM[['Data de conclusão', 'PROJETO', 'Status', 'Operação', 'Supervisor', 'MUNICIPIO']]
                carteira_CCM['PROJETO'] = carteira_CCM['PROJETO'].str.replace('B-', '')
                carteira_CCM = carteira_CCM.query("PROJETO != ''")
                carteira_CCM.columns = ['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'MUNICÍPIO']
                carteira_CCM = carteira_CCM[carteira_CCM['STATUS GERAL'].isin(['CONCLUÍDA', 'Concluída', 'CONCLUIDA', '-CONCLUIDA'])]
                print('lendo carteira_CCM')
                
                sup = []
                diaC = self.le_planilha_google(configs.diaC, "Dia C", 'B4:J')
                diaC = diaC[['Dt. Conclusão', 'Projeto', 'Situação', 'Unidade', 'Município']]
                diaC['Projeto'] = diaC['Projeto'].str.replace('B-', '')
                diaC = diaC.query("Projeto != ''")
                for i, row in diaC.iterrows():
                    sup.append('')
                diaC['SUPERVISOR'] = sup
                diaC = diaC[['Dt. Conclusão', 'Projeto', 'Situação', 'Unidade', 'SUPERVISOR', 'Município']]
                diaC.columns = ['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'MUNICÍPIO']
                diaC = diaC[diaC['STATUS GERAL'].isin(['CONCLUÍDA', 'Concluída', 'CONCLUIDA', '-CONCLUIDA'])]
                print('lendo diaC')
                break
            except Exception as e:
                print(e)
                sleep(62)
                pass

        
        #carteira_barreiras, 
        carteira_geral = pd.concat([carteira_g, carteira_barreiras, carteira_CCM, diaC], ignore_index = True)
        carteira_geral['STATUS GERAL'] = carteira_geral['STATUS GERAL'].str.replace(' ', '')
        obras_concluidas_completo = carteira_geral

        # Filtro para a carteira
        datas_para_filtrar = ['01/01/2023', '01/02/2023', '01/03/2023', '01/04/2023', '01/05/2023', '01/06/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[~obras_concluidas_completo['CARTEIRA'].isin(datas_para_filtrar)]
        obras_concluidas = (((obras_concluidas_completo['PROJETO'].drop_duplicates())))

        obras_concluidas_formatado = []
        for i in obras_concluidas:
            i = str(i)
            i = i.replace('B-', '').replace('/PIVO', '').replace('-PIVO', '').replace('/JUDICIAL', '').replace('Y-', '')

            i = int(i)
            obras_concluidas_formatado.append(i)

        obras_concluidas = obras_concluidas_formatado

        ####################### LENDO PLANILHA DO FECHAMENTO
        while True:
            try:
                obras_recepcionadas_resolucao = self.le_planilha_google(configs.id_planilha_planejamento, "Obras em resolução de problema")
                obras_recepcionadas_resolucao = obras_recepcionadas_resolucao.query("PROJETO != ''")
                obras_recepcionadas_resolucao = obras_recepcionadas_resolucao['PROJETO']
                print('obras_recepcionadas_resolucao')

                obras_recepcionadas_vtc = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS CONQUISTA")
                obras_recepcionadas_vtc = obras_recepcionadas_vtc.query("PROJETO != ''")
                obras_recepcionadas_vtc = obras_recepcionadas_vtc['PROJETO']
                print('obras_recepcionadas_vtc')

                obras_recepcionadas_jeq = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS JEQUIE")
                obras_recepcionadas_jeq = obras_recepcionadas_jeq.query("PROJETO != ''")
                obras_recepcionadas_jeq = obras_recepcionadas_jeq['PROJETO']
                print('obras_recepcionadas_jeq')

                obras_recepcionadas_bjl = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS LAPA")
                obras_recepcionadas_bjl = obras_recepcionadas_bjl.query("PROJETO != ''")
                obras_recepcionadas_bjl = obras_recepcionadas_bjl['PROJETO']
                print('obras_recepcionadas_bjl')

                obras_recepcionadas_ire = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS IRECE")
                obras_recepcionadas_ire = obras_recepcionadas_ire.query("PROJETO != ''")
                obras_recepcionadas_ire = obras_recepcionadas_ire['PROJETO']
                print('obras_recepcionadas_ire')

                obras_recepcionadas_gbi = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS GUANAMBI")
                obras_recepcionadas_gbi = obras_recepcionadas_gbi.query("PROJETO != ''")
                obras_recepcionadas_gbi = obras_recepcionadas_gbi['PROJETO']
                print('obras_recepcionadas_gbi')
                
                obras_recepcionadas_brr = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS BARREIRAS")
                obras_recepcionadas_brr = obras_recepcionadas_brr.query("PROJETO != ''")
                obras_recepcionadas_brr = obras_recepcionadas_brr['PROJETO']
                print('obras_recepcionadas_brr')
                
                obras_recepcionadas_ibt = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS IBOTIRAMA")
                obras_recepcionadas_ibt = obras_recepcionadas_ibt.query("PROJETO != ''")
                obras_recepcionadas_ibt = obras_recepcionadas_ibt['PROJETO']
                print('obras_recepcionadas_ibt')
                
                obras_recepcionadas_bru = self.le_planilha_google(configs.id_planilha_planejamento, "OBRAS BRUMADO")
                obras_recepcionadas_bru = obras_recepcionadas_bru.query("PROJETO != ''")
                obras_recepcionadas_bru = obras_recepcionadas_bru['PROJETO']
                print('obras_recepcionadas_bru')
                break
            except Exception as e:
                print(e)
                sleep(62)
                pass
        
        obras_recepcionadas_geral = pd.concat([obras_recepcionadas_resolucao, obras_recepcionadas_vtc, obras_recepcionadas_jeq , obras_recepcionadas_brr, obras_recepcionadas_gbi, obras_recepcionadas_bjl, obras_recepcionadas_ire, obras_recepcionadas_ibt, obras_recepcionadas_bru], ignore_index = True)
        
        cont = 0
        for i in obras_recepcionadas_geral:
            if i == 'PROJETO':
                obras_recepcionadas_geral.drop(cont)
                continue
            i = i.replace(' ', '')
            if ((i != None) and (i != '')):
                obras_recepcionadas_geral[cont] = int(i[2:9])
            cont += 1
        
        obras_concluidas_sem_pasta_no_fechamento = []
        obras_concluidas = obras_concluidas
        obras_recepcionadas_geral = obras_recepcionadas_geral.tolist()

        
        ####################### CONFERE QUAIS OBRAS JÁ ESTÃO NA PLANILHA DO FECHAMENTO
        cont = 0
        for cont, i in enumerate(obras_concluidas): 
            if i in obras_recepcionadas_geral:
                pass
            else:
                obras_concluidas_sem_pasta_no_fechamento.append(obras_concluidas[cont])
            cont += 1

        ####################### CONFERE QUAIS OBRAS ESTÃO PENDENTES DE ENVIO DA PASTA NO GEOEX

        qtd = len(obras_concluidas_sem_pasta_no_fechamento)

        jequie = ['CRAVOLÂNDIA', 'BREJÕES', 'IRAJUBA', 'ITAQUARA', 'ITIRUÇU', 'JAGUAQUARA', 'JEQUIÉ', 'LAFAIETE COUTINHO', 'LAJEDO DO TOBOCAL', 'MANOEL VITORINO', 'MARACÁS', 'NOVA ITARANA', 'PLANALTINO', 'SANTA INÊS', 'LAFAIETE COUTINHO']

        vitoria_da_conquista = ['ANAGÉ', 'BARRA DO CHOÇA', 'BELO CAMPO', 'BOA NOVA', 'BOM JEJUS DA SERRA', 'CAETANOS', 'CÂNDIDO SALES', 'CARAÍBAS', 'CONDEÚBA', 'CORDEIROS', 'ENCRUZILHADA', 'MAETINGA', 'MIRANTE', 'PIRIPÁ', 'PLANALTO', 'POÇÕES', 'PRESIDENTE JANIO QUADROS', 'TREMEDAL', 'VITÓRIA DA CONQUISTA']

        itapetinga = ['MACARANI', 'CAATIBA', 'FIRMINO ALVES', 'IBICUÍ', 'IGUAÍ', 'ITAMBÉ', 'ITAPETINGA', 'ITARANTIM', 'ITORORÓ', 'MAIQUINIQUE', 'NOVA CANAÃ', 'POTIRAGUÁ', 'RIBEIRÃO DO LARGO']

        barreiras = ['ANGICAL', 'BAIANÓPOLIS', 'BARREIRAS', 'CATOLÂNDIA', 'COTEGIPE', 'CRISTÓPOLIS', 'FORMOSA DO RIO PRETO', 'LUIS EDUARDO MAGALHÃES', 'RIACHÃO DAS NEVES', 'SANTA RITA DE CÁSSIA', 'SÃO DESIDÉRIO', 'WANDERLEY']

        ibotirama = ['IBOTIRAMA', 'MUQUEM DO SÃO FRANCISCO', 'OLIVEIRA DOS BREJINHOS', 'BARRA', 'BURITIRAMA', 'MORPARÁ', 'BROTAS DE MACAÚBAS', 'IPUPIARA', 'MANSIDÃO', 'BOQUIRA', 'MACAÚBAS', 'IBITIARA', 'NOVO HORIZONTE', 'IBIPITANGA']

        bom_jesus_da_lapa = ['BOM JESUS DA LAPA', 'PARATINGA', 'RIACHO DE SANTANA', 'MATINA', 'SERRA DO RAMALHO', 'SÍTIO DO MATO', 'SANTANA', 'CANÁPOLIS', 'SERRA DOURADA', 'TABOCAS DO BREJO VELHO', 'BREJOLÂNDIA', 'SANTA MARIA DA VITORIA', 'SÃO FÉLIX DO CORIBE', 'JABORANDI', 'CORIBE', 'COCOS', 'FEIRA DA MATA', 'CORRENTINA']

        guanambi = ['CAETITÉ', 'CANDIBA', 'CARINHANHA', 'FEIRA DA MATA', 'GUANAMBI', 'IGAPORÃ', 'IUIÚ', 'JACARACI', 'LICÍNIO DE ALMEIDA', 'MALHADA', 'MATINA', 'MORTUGABA', 'PALMAS DE MONTE ALTO', 'PINDAÍ', 'RIACHO DE SANTANA', 'SEBASTIÃO LARANJEIRAS', 'URANDI']

        irece = ['AMÉRICA DOURADA', 'BARRA DO MENDES', 'BARRO ALTO', 'CAFARNAUM', 'CENTRAL', 'GENTIO DO OURO', 'IBIPEBA', 'IBITITÁ', 'IRECÊ', 'ITAGUAÇU DA BAHIA', 'JOÃO DOURADO', 'JUSSARA', 'LAPÃO', 'MORRO DO CHAPÉU', 'MULUNGU DO MORRO', 'PRESIDENTE DUTRA', 'SÃO GABRIEL', 'UIBAÍ', 'XIQUE-XIQUE']
        
        livramento = ['RIO DE CONTAS', 'ÉRICO CARDOSO', 'CATURAMA', 'RIO DO PIRES']
        
        status_aceitos = ['CRIADO', 'CANCELADO', 'ACEITO', 'ACEITO COM RESTRIÇÕES', 'REJEITADO', 'VALIDADO']
        projetos_pendente_asbuilt = [['UNIDADE', 'PROJETO', 'TÍTULO', 'VALOR DO PROJETO', 'DATA DE ENERGIZAÇÃO', 'SUPERVISOR', 'MUNICÍPIO']]
        url = 'Cadastro/ConsultarProjeto/Item'
        
        statuspastaid = {
            1 : 'CRIADO',
            6 : 'CANCELADO',
            30 : 'ACEITO',
            31 : 'ACEITO COM RESTRIÇÕES',
            32 : 'REJEITADO',
            35 : 'VALIDADO'
        }
        
        def fazer_requisicao(url, body):
            """Função auxiliar para realizar requisições POST."""
            resposta = ''
            fim = False
            
            while True:
                r = GeoexHook(cookie_path).run("POST", url, json=body)
                    
                if r.status_code!=200:
                    print(' Erro na requisição: Code: '+str(r.status_code)+', Reason: '+str(r.reason))
                    sleep(30)
                    fim = True
                    continue
                if fim:
                    fim = False
                    print(' Erro na requisição: Code: '+str(r.status_code)+', Reason: '+str(r.reason))
                try:
                    resposta = r.json()
                except Exception as e:
                    print(r)
                    raise e
                break
            
            if resposta["IsUnauthorized"]:
                print(Fore.RED + resposta)
                raise "Cookie Inválido"
            #print(resposta)
            return resposta

        database = "dags/BOB_V2/db.csv"

        df = pd.read_csv(database)

        x = 1
        for i in obras_concluidas_sem_pasta_no_fechamento:
            if i in df['external_id'].values:
                print(f'Projeto já existe, pulando: {i} - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})')
                x += 1
                continue
            
            #sleep(2)
            resposta = fazer_requisicao(url=url, body={'id': str(i)})
            
            try:
                #resposta = resposta.json()
                id_projeto = resposta['Content']['ProjetoId']
                    
                url_pasta = 'ConsultarProjeto/EnvioPasta/Itens'
                body_pasta = {'ProjetoId': id_projeto}
                
                resposta_pasta = fazer_requisicao(url=url_pasta, body=body_pasta)
                conteudo_pasta = resposta_pasta['Content']['Envios']
                envio = []
                for j in conteudo_pasta:
                    if j['EmpresaId']==70:
                        envio = j['Ultimo']
                        break
                    else:
                        envio = 'PENDENTE'
                        
                if envio=='PENDENTE' or envio==None or envio==[]:
                    status_pasta = 'PENDENTE'
                    #print(envio, status_pasta)
                else:
                    status_pasta = statuspastaid.get(envio['HistoricoStatusId'],envio['HistoricoStatusId'])
                    #print(envio, status_pasta)

                #status_pasta = resposta['Content']['EnvioPastaStatus']
                if not(status_pasta in status_aceitos) and str(i)!='B-1130987':
                    try:
                        vl_projeto = resposta['Content']['VlProjeto']
                    except:
                        vl_projeto = ''
                    try:
                        titulo = resposta['Content']['Titulo']
                    except:
                        titulo = ''
                    try:
                        data_energ = resposta['Content']['DtZps09'][0:10]
                        data_energ = datetime.strptime(data_energ, "%Y-%m-%d")
                        data_energ = data_energ.strftime("%d/%m/%y")
                    except:
                        data_energ = ''
                    
                    try: 
                        unidade = resposta['Content']['Municipio']
                        if not unidade:
                            unidade = ''
                        else:
                            if unidade in jequie:
                                unidade = 'JEQUIÉ'
                            elif unidade in ibotirama:
                                unidade = 'IBOTIRAMA'
                            elif unidade in bom_jesus_da_lapa:
                                unidade = 'BOM JESUS DA LAPA'
                            elif unidade in guanambi:
                                unidade = 'GUANAMBI'
                            elif unidade in irece:
                                unidade = 'IRECÊ'
                            elif unidade in vitoria_da_conquista:
                                unidade = 'VITÓRIA DA CONQUISTA'
                            elif unidade in barreiras:
                                unidade = 'BARREIRAS'
                            elif unidade in itapetinga:
                                unidade = 'ITAPETINGA'
                            elif unidade in livramento:
                                unidade = 'LIVRAMENTO'
                            else:
                                unidade = unidade
                    except:
                        unidade = ''

                    supervisor = obras_concluidas_completo.loc[obras_concluidas_completo['PROJETO'] == str(i)]['SUPERVISOR'].values
                    if supervisor.size > 0:
                        supervisor = supervisor[-1]
                    else:
                        supervisor = ''
                    if supervisor[0:3] == 'SUP':
                        supervisor = supervisor[8:]

                    try: 
                        municipio = resposta['Content']['Municipio']
                        if not municipio:
                            municipio = ''
                    except:
                        municipio = ''
                    
                    projetos_pendente_asbuilt.append([unidade, i, titulo, vl_projeto, data_energ, supervisor, municipio])
                    print(Fore.GREEN + f'\n{status_pasta} - {i} - {unidade} - {municipio} - {titulo} - {data_energ} - {vl_projeto} - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})')
                else:
                    project_name = resposta['Content'].get('Titulo', '')
                    df.loc[len(df)] = [i, project_name, status_pasta]
                    print(Fore.RED + f'\n{status_pasta} - {i} - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})')
            except Exception as e:
                print('\nsem acesso ao projeto ', i)
                print(resposta)
                print(e)

            x += 1

        data_frame = pd.DataFrame(projetos_pendente_asbuilt)
        data_frame[[1]] = data_frame[[1]].drop_duplicates()
        data_frame = data_frame.dropna()
        print(data_frame)

        while True:
            try:
                dados_list = [data_frame.columns.values.tolist()] + data_frame.values.tolist()

                sh = self.GS_SERVICE.open_by_key('1GQ5pLG2DddGrEuRJILe-3g_Rwzhg-82EkVFZnX1_we4')
                pastas_pendentes = sh.worksheet('pastas pendentes')

                pastas_pendentes.clear()
                pastas_pendentes.update(values=dados_list, range_name='A1')
                sh.worksheet('data atualização').update(range_name='A1', values=[[datetime.now(br_tz).strftime("%d/%m/%Y %H:%M")]])
                break
            except Exception as e:
                print(e)
