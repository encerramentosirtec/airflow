import os
import json
import gspread
import traceback
import pandas as pd
from time import sleep
from src.geoex import Geoex
from pendulum import timezone
from datetime import datetime
from src.config import configs
from src.geoex import Geoex, GeoexHook
from src.google_sheets import GoogleSheets

class Bots:

    def __init__(self, cookie_file='cookie_ccm.json', cred_file='causal_scarab.json'):
        #self.PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código
        self.PATH = os.getenv('AIRFLOW_HOME')
        
        with open(os.path.join(self.PATH, f'assets/auth_geoex/{cookie_file}'), 'r') as f:
            self.cookie = json.load(f)
            
        self.geoex = Geoex(cookie_file)
        self.cred_file = cred_file
        self.GS_SERVICE = gspread.service_account(filename=os.path.join(os.getcwd(), f'assets/auth_google/{cred_file}'))
        self.br_tz = timezone("Brazil/East")

        self.gs = GoogleSheets(cred_file)

        self.geoex_hook = GeoexHook(self.cookie)

        self.url_geo = 'Cadastro/ConsultarProjeto/Item'
        self.url_pasta = 'ConsultarProjeto/EnvioPasta/Itens'
        self.url_termo_geo = 'ConsultarProjeto/TermoGeo/Itens'
        self.url_encerramento = 'ConsultarProjeto/EncerramentoOnline'

        self.statuspastaid = {
            0 : 'NÃO ENVIADO',
            1 : 'CRIADO',
            6 : 'CANCELADO',
            22 : 'PENDENTE',
            30 : 'ACEITO',
            31 : 'ACEITO COM RESTRIÇÕES',
            32 : 'REJEITADO',
            35 : 'VALIDADO'
        }



    def le_planilha_google(self, url, aba, intervalo=None, render_option='UNFORMATTED_VALUE'):
        try:
            sh = self.GS_SERVICE.open_by_url(url)
        except:
            sh = self.GS_SERVICE.open_by_key(url)
        ws = sh.worksheet(aba)
        if intervalo == None:
            df = ws.get_all_values(value_render_option=render_option)
        else:
            df = ws.get_all_values(range_name=intervalo, value_render_option=render_option)
        df = pd.DataFrame(df, columns = df.pop(0))

        return df

    def le_celula_google(self, url, aba, intervalo):
        try:
            sh = self.GS_SERVICE.open_by_url(url)
        except:
            sh = self.GS_SERVICE.open_by_key(url)
        ws = sh.worksheet(aba)
        df = ws.acell(intervalo).value

        return df

    def fazer_requisicao(self, url, body):
        '''resposta = ''
        
        r = self.geoex_hook.run("POST", url, json=body)
        resposta = r.json()
        
        if resposta["IsUnauthorized"] or resposta['StatusCode']==403:
            print('Cookie Inválido')
            print(resposta)
            raise TypeError('Cookie Inválido')'''
        

        while True:
            r = self.geoex_hook.run("POST", url, json=body)
            if r.status_code != 341:
                break
            print('Neoex em atualização')
            sleep(30)

        if r.status_code != 200:
            print(f"Erro na requisição: Status {r.status_code}")
            print(f"Resposta bruta: {r.text[:200]}") # Loga o início da resposta para depuração
            return {"IsUnauthorized": False, "StatusCode": r.status_code, "Content": None}

        try:
            resposta = r.json()
        except Exception:
            print("A resposta não é um JSON válido")
            return {"IsUnauthorized": False, "StatusCode": 500, "Content": None}
        
        if resposta.get("IsUnauthorized") or resposta.get('StatusCode') == 403:
            print(f'Cookie Inválido - Status Code: {resposta.get("StatusCode", "N/A")}, Mensagem: {resposta.get("Message", "N/A")}')
            raise TypeError('Cookie Inválido')

        
        return resposta


    # As-Built
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
                
                espelho_CCM = self.le_planilha_google(configs.espelho_CCM, "Base de dados (Espelho)", 'B2:Y')
                espelho_CCM = espelho_CCM[['Dt. En. GEOEX', 'Projeto', 'Status Execucao', 'UNIDADE', 'Supervisor', 'Valor Considerado']]
                espelho_CCM['Projeto'] = espelho_CCM['Projeto'].str.replace('B-', '')
                espelho_CCM.columns = ['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'VALOR']
                espelho_CCM = espelho_CCM[espelho_CCM['STATUS GERAL'].isin(['CONCLUÍDA']) | espelho_CCM['PROJETO'].isin(carteira_g['PROJETO'])]
                espelho_CCM = espelho_CCM.query("PROJETO != ''")
                espelho_CCM = espelho_CCM.drop_duplicates(subset=['PROJETO'])
                espelho_CCM['PROJETO'] = pd.to_numeric(espelho_CCM['PROJETO'], errors='coerce')
                espelho_CCM = espelho_CCM.dropna()
                espelho_CCM['MUNICÍPIO'] = ['-'] * len(espelho_CCM)
                subespelho = espelho_CCM[['CARTEIRA', 'PROJETO', 'STATUS GERAL', 'UNIDADE', 'SUPERVISOR', 'MUNICÍPIO']]
                print('lendo espelho_CCM')
                break
            except Exception as e:
                if isinstance(e, KeyError):
                    # Se for um KeyError, relança a exceção para PARAR o programa
                    print("\nERRO CRÍTICO: KeyError detectado. O programa será encerrado.")
                    raise e
                traceback.print_exc()
                sleep(62)
                pass

        
        #carteira_barreiras, 
        carteira_geral = pd.concat([carteira_g, carteira_barreiras, carteira_CCM, diaC, subespelho], ignore_index = True)
        carteira_geral['STATUS GERAL'] = carteira_geral['STATUS GERAL'].str.replace(' ', '')
        obras_concluidas_completo = carteira_geral

        # Filtro para a carteira
        datas_para_filtrar = ['01/01/2023', '01/02/2023', '01/03/2023', '01/04/2023', '01/05/2023', '01/06/2023']
        obras_concluidas_completo = obras_concluidas_completo.loc[~obras_concluidas_completo['CARTEIRA'].isin(datas_para_filtrar)]
        obras_concluidas = obras_concluidas_completo['PROJETO'].drop_duplicates().copy()
        obras_concluidas = obras_concluidas.dropna()
        #print(obras_concluidas)

        #obras_concluidas_formatado = []
        
        '''for i in obras_concluidas:
            try:
                i = int(i)
            except Exception as e:
                i = str(i)
                i = i.replace('B-', '').replace('/PIVO', '').replace('-PIVO', '').replace('/JUDICIAL', '').replace('Y-', '')
                i = int(i)
            obras_concluidas_formatado.append(i)
        
        obras_concluidas = obras_concluidas_formatado'''

        padrao = r'B-|/PIVO|-PIVO|/JUDICIAL|Y-'

        obras_concluidas_formatado = obras_concluidas.astype(str).str.replace(padrao, '', regex=True).copy()
        obras_concluidas_formatado = pd.to_numeric(obras_concluidas_formatado, errors='coerce')
        obras_concluidas_formatado = obras_concluidas_formatado.drop_duplicates().dropna().astype(int)

        ####################### LENDO PLANILHA DO FECHAMENTO
        while True:
            try:
                '''obras_recepcionadas_resolucao = self.le_planilha_google(configs.id_planilha_postagemV5, "Obras em resolução de problema")
                obras_recepcionadas_resolucao = obras_recepcionadas_resolucao.query("PROJETO != ''")
                obras_recepcionadas_resolucao = obras_recepcionadas_resolucao['PROJETO']
                print('obras_recepcionadas_resolucao')
                sleep(5)'''

                obras_recepcionadas_vtc = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS CONQUISTA")
                obras_recepcionadas_vtc = obras_recepcionadas_vtc.query("PROJETO != ''")
                obras_recepcionadas_vtc = obras_recepcionadas_vtc['PROJETO']
                print('obras_recepcionadas_vtc')
                sleep(5)

                obras_recepcionadas_jeq = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS JEQUIE")
                obras_recepcionadas_jeq = obras_recepcionadas_jeq.query("PROJETO != ''")
                obras_recepcionadas_jeq = obras_recepcionadas_jeq['PROJETO']
                print('obras_recepcionadas_jeq')
                sleep(5)

                obras_recepcionadas_bjl = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS LAPA")
                obras_recepcionadas_bjl = obras_recepcionadas_bjl.query("PROJETO != ''")
                obras_recepcionadas_bjl = obras_recepcionadas_bjl['PROJETO']
                print('obras_recepcionadas_bjl')
                sleep(5)

                obras_recepcionadas_ire = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS IRECE")
                obras_recepcionadas_ire = obras_recepcionadas_ire.query("PROJETO != ''")
                obras_recepcionadas_ire = obras_recepcionadas_ire['PROJETO']
                print('obras_recepcionadas_ire')
                sleep(5)

                obras_recepcionadas_gbi = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS GUANAMBI")
                obras_recepcionadas_gbi = obras_recepcionadas_gbi.query("PROJETO != ''")
                obras_recepcionadas_gbi = obras_recepcionadas_gbi['PROJETO']
                print('obras_recepcionadas_gbi')
                sleep(5)
                
                obras_recepcionadas_brr = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS BARREIRAS")
                obras_recepcionadas_brr = obras_recepcionadas_brr.query("PROJETO != ''")
                obras_recepcionadas_brr = obras_recepcionadas_brr['PROJETO']
                print('obras_recepcionadas_brr')
                sleep(5)
                
                obras_recepcionadas_ibt = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS IBOTIRAMA")
                obras_recepcionadas_ibt = obras_recepcionadas_ibt.query("PROJETO != ''")
                obras_recepcionadas_ibt = obras_recepcionadas_ibt['PROJETO']
                print('obras_recepcionadas_ibt')
                sleep(5)
                
                obras_recepcionadas_bru = self.le_planilha_google(configs.id_planilha_postagemV5, "OBRAS BRUMADO")
                obras_recepcionadas_bru = obras_recepcionadas_bru.query("PROJETO != ''")
                obras_recepcionadas_bru = obras_recepcionadas_bru['PROJETO']
                print('obras_recepcionadas_bru')
                break
            except Exception as e:
                print(e)
                sleep(62)
                pass
        
        #obras_recepcionadas_geral = pd.concat([obras_recepcionadas_resolucao, obras_recepcionadas_vtc, obras_recepcionadas_jeq , obras_recepcionadas_brr, obras_recepcionadas_gbi, obras_recepcionadas_bjl, obras_recepcionadas_ire, obras_recepcionadas_ibt, obras_recepcionadas_bru], ignore_index = True)
        obras_recepcionadas_geral = pd.concat([obras_recepcionadas_vtc, obras_recepcionadas_jeq , obras_recepcionadas_brr, obras_recepcionadas_gbi, obras_recepcionadas_bjl, obras_recepcionadas_ire, obras_recepcionadas_ibt, obras_recepcionadas_bru], ignore_index = True)
        
        '''cont = 0
        for i in obras_recepcionadas_geral:
            if i == 'PROJETO':
                obras_recepcionadas_geral.drop(cont)
                continue
            i = str(i).replace(' ', '')
            if ((i != None) and (i != '')):
                obras_recepcionadas_geral[cont] = int(i[2:9])
            cont += 1'''
        
        # Filtra para remover qualquer linha que seja 'PROJETO' ou vazia/nula
        obras_recepcionadas_geral = obras_recepcionadas_geral[
            (obras_recepcionadas_geral != 'PROJETO') & 
            (obras_recepcionadas_geral.notna()) & 
            (obras_recepcionadas_geral.astype(str).str.strip() != '')
        ].copy()

        obras_recepcionadas_geral = (
            obras_recepcionadas_geral.astype(str)
            .str.replace(' ', '')
            .str[2:9]
            .astype(int)
        )

        obras_concluidas_sem_pasta_no_fechamento = []
        obras_concluidas = obras_concluidas_formatado.copy()
        obras_recepcionadas_geral = obras_recepcionadas_geral.tolist()

        
        ####################### CONFERE QUAIS OBRAS JÁ ESTÃO NA PLANILHA DO FECHAMENTO
        '''cont = 0
        for cont, i in enumerate(obras_concluidas): 
            if i in obras_recepcionadas_geral or int(i)==1063382:
                pass
            else:
                obras_concluidas_sem_pasta_no_fechamento.append(obras_concluidas[cont])
            cont += 1'''

        obras_concluidas_sem_pasta_no_fechamento = [
            obra for obra in obras_concluidas 
            if obra not in obras_recepcionadas_geral and int(obra) != 1063382
        ]

        ####################### CONFERE QUAIS OBRAS ESTÃO PENDENTES DE ENVIO DA PASTA NO GEOEX

        lista_cidades = {
            'JEQUIÉ' : ['MARCIONÍLIO DE SOUZA', 'CRAVOLÂNDIA', 'BREJÕES', 'IRAJUBA', 'ITAQUARA', 'ITIRUÇU', 'JAGUAQUARA', 'JEQUIÉ', 'LAFAIETE COUTINHO', 'LAJEDO DO TOBOCAL', 'MANOEL VITORINO', 'MARACÁS', 'NOVA ITARANA', 'PLANALTINO', 'SANTA INÊS', 'LAFAIETE COUTINHO'],
            'VITÓRIA DA CONQUISTA' : ['ANAGÉ', 'BARRA DO CHOÇA', 'BELO CAMPO', 'BOA NOVA', 'BOM JEJUS DA SERRA', 'CAETANOS', 'CÂNDIDO SALES', 'CARAÍBAS', 'CONDEÚBA', 'CORDEIROS', 'ENCRUZILHADA', 'MAETINGA', 'MIRANTE', 'PIRIPÁ', 'PLANALTO', 'POÇÕES', 'PRESIDENTE JANIO QUADROS', 'TREMEDAL', 'VITÓRIA DA CONQUISTA', 'BOM JESUS DA SERRA'],
            'ITAPETINGA' : ['MACARANI', 'CAATIBA', 'FIRMINO ALVES', 'IBICUÍ', 'IGUAÍ', 'ITAMBÉ', 'ITAPETINGA', 'ITARANTIM', 'ITORORÓ', 'MAIQUINIQUE', 'NOVA CANAÃ', 'POTIRAGUÁ', 'RIBEIRÃO DO LARGO'],
            'BARREIRAS' : ['ANGICAL', 'BAIANÓPOLIS', 'BARREIRAS', 'CATOLÂNDIA', 'COTEGIPE', 'CRISTÓPOLIS', 'FORMOSA DO RIO PRETO', 'LUIS EDUARDO MAGALHÃES', 'RIACHÃO DAS NEVES', 'SANTA RITA DE CÁSSIA', 'SÃO DESIDÉRIO', 'WANDERLEY'],
            'IBOTIRAMA' : ['IBOTIRAMA', 'MUQUEM DO SÃO FRANCISCO', 'OLIVEIRA DOS BREJINHOS', 'BARRA', 'BURITIRAMA', 'MORPARÁ', 'BROTAS DE MACAÚBAS', 'IPUPIARA', 'MANSIDÃO', 'BOQUIRA', 'MACAÚBAS', 'IBITIARA', 'NOVO HORIZONTE', 'IBIPITANGA'],
            'BOM JESUS DA LAPA' : ['BOM JESUS DA LAPA', 'PARATINGA', 'RIACHO DE SANTANA', 'MATINA', 'SERRA DO RAMALHO', 'SÍTIO DO MATO', 'SANTANA', 'CANÁPOLIS', 'SERRA DOURADA', 'TABOCAS DO BREJO VELHO', 'BREJOLÂNDIA', 'SANTA MARIA DA VITORIA', 'SÃO FÉLIX DO CORIBE', 'JABORANDI', 'CORIBE', 'COCOS', 'FEIRA DA MATA', 'CORRENTINA'],
            'GUANAMBI' : ['CAETITÉ', 'CANDIBA', 'CARINHANHA', 'FEIRA DA MATA', 'GUANAMBI', 'IGAPORÃ', 'IUIÚ', 'JACARACI', 'LICÍNIO DE ALMEIDA', 'MALHADA', 'MATINA', 'MORTUGABA', 'PALMAS DE MONTE ALTO', 'PINDAÍ', 'RIACHO DE SANTANA', 'SEBASTIÃO LARANJEIRAS', 'URANDI'],
            'IRECÊ' : ['SOUTO SOARES', 'CANARANA', 'AMÉRICA DOURADA', 'BARRA DO MENDES', 'BARRO ALTO', 'CAFARNAUM', 'CENTRAL', 'GENTIO DO OURO', 'IBIPEBA', 'IBITITÁ', 'IRECÊ', 'ITAGUAÇU DA BAHIA', 'JOÃO DOURADO', 'JUSSARA', 'LAPÃO', 'MORRO DO CHAPÉU', 'MULUNGU DO MORRO', 'PRESIDENTE DUTRA', 'SÃO GABRIEL', 'UIBAÍ', 'XIQUE-XIQUE'],
            'LIVRAMENTO' : ['ABAÍRA', 'RIO DE CONTAS', 'ÉRICO CARDOSO', 'CATURAMA', 'RIO DO PIRES', 'PIATÃ'],
            'BRUMADO' : ['MALHADA DE PEDRAS', 'ARACATU', 'GUAJERU', 'CACULÉ', 'LIVRAMENTO DE NOSSA SENHORA', 'JUSSIAPÊ', 'LIVRAMENTO', 'DOM BASÍLIO', 'IBIASSUCÊ', 'ITUAÇU', 'BRUMADO', 'BOTUPORÃ', 'RIO DO ANTÔNIO', 'LAGOA REAL'],
        }

        df_cidades = []

        for regiao, cidades in lista_cidades.items():
            for cidade in cidades:
                df_cidades.append({'REGIÃO': regiao, 'CIDADE': cidade})

        df_cidades = pd.DataFrame(df_cidades)
        busca_rapida = df_cidades.set_index('CIDADE')['REGIÃO'].to_dict()

        status_aceitos = ['CRIADO', 'CANCELADO', 'ACEITO', 'ACEITO COM RESTRIÇÕES', 'REJEITADO', 'VALIDADO']
        projetos_pendente_asbuilt = [['UNIDADE', 'PROJETO', 'TÍTULO', 'VALOR DO PROJETO', 'DATA DE ENERGIZAÇÃO', 'SUPERVISOR', 'MUNICÍPIO']]
        
        statuspastaid = {
            1 : 'CRIADO',
            6 : 'CANCELADO',
            30 : 'ACEITO',
            31 : 'ACEITO COM RESTRIÇÕES',
            32 : 'REJEITADO',
            35 : 'VALIDADO'
        }
        
        database = 'assets/db/db.csv'

        df = pd.read_csv(database)
        zerados = [1125950, 1193199, 1168391, 1222485, 1231902, 1183205, 1227488, 1228610, 1230135, 1231959, 972378, 1156314, 1131643, 1207605, 1201395, 1195842, 1247528, 1207189, 1145820, 1102510, 1163605, 1143926, 1214254, 1125940]
        excluidos = [1130987]

        x = 1
        for i in obras_concluidas_sem_pasta_no_fechamento:
            if i in df['external_id'].values or int(i) in excluidos:
                #print(f'Projeto já existe, pulando: {i} - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})')
                x += 1
                continue
            
            '''try:
                resposta = self.fazer_requisicao(url=self.url_geo, body={'id': str(i)})
            except Exception as e:
                print(i)
                traceback.print_exc()
                raise e'''
            
            try:
                resposta = self.fazer_requisicao(url=self.url_geo, body={'id': str(i)})
                # Se a resposta vier vazia ou com erro, pula para o próximo projeto
                if not resposta or resposta.get('Content') is None:
                    print(f"Pulando projeto {i} devido a resposta inválida. - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})")
                    x += 1
                    continue
            except Exception as e:
                print(f"Falha crítica no projeto {i}: {e}")
                if "Cookie Inválido" in str(e):
                    print("Cookie inválido detectado. Encerrando o processo.")
                    raise e  # Encerra o processo se o cookie for inválido
                # Opcional: sleep curto para evitar flood em caso de instabilidade de rede
                sleep(2)
                x += 1
                continue
                        
            try:
                id_projeto = resposta['Content']['ProjetoId']
                body_pasta = {'ProjetoId': id_projeto}
                
                try:
                    resposta_pasta = self.fazer_requisicao(url=self.url_pasta, body=body_pasta)
                except Exception as e:
                    print(i)
                    traceback.print_exc()
                    raise e
                
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
                else:
                    status_pasta = statuspastaid.get(envio['HistoricoStatusId'],envio['HistoricoStatusId'])

                if not(status_pasta in status_aceitos):
                    try:
                        if int(i) in zerados:
                            if int(i) == 1125950:
                                vl_projeto = 651937.25
                            else:
                                vl_projeto = ''
                        else:
                            vl_projeto = espelho_CCM.loc[espelho_CCM["PROJETO"] == int(i), "VALOR"].values[0]
                    except Exception as e:
                        print("erro buscando valor: ", i, type(i))
                        traceback.print_exc()
                        vl_projeto = ''
                    try:
                        titulo = resposta['Content']['Titulo']
                    except Exception as e:
                        print("erro buscando titulo: ")
                        traceback.print_exc()
                        titulo = ''
                    try:
                        data_energ = resposta['Content']['DtZps09'][0:10]
                        data_energ = datetime.strptime(data_energ, "%Y-%m-%d")
                        data_energ = data_energ.strftime("%d/%m/%y")
                    except Exception as e:
                        print("erro buscando data de energização: ")
                        traceback.print_exc()
                        data_energ = ''
                    
                    try: 
                        unidade = resposta['Content']['Municipio']
                        municipio = resposta['Content']['Municipio']
                        if not unidade:
                            unidade = ''
                        else:
                            unidade = busca_rapida.get(unidade)
                    except Exception as e:
                        print("erro buscando unidade: ")
                        traceback.print_exc()
                        unidade = ''

                    supervisor = obras_concluidas_completo.loc[obras_concluidas_completo['PROJETO'] == str(i)]['SUPERVISOR'].values
                    if supervisor.size > 0:
                        supervisor = supervisor[-1]
                    else:
                        supervisor = ''
                    if supervisor[0:3] == 'SUP':
                        supervisor = supervisor[8:]
                    
                    projetos_pendente_asbuilt.append([unidade, i, titulo, vl_projeto, data_energ, supervisor, municipio])
                    print(f'{status_pasta} - {i} - {unidade} - {municipio} - {titulo} - {data_energ} - {vl_projeto} - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})')
                else:
                    project_name = resposta['Content'].get('Titulo', '')
                    df.loc[len(df)] = [i, project_name, status_pasta]
                    #print(f'\n{status_pasta} - {i} - ({x}/{str(len(obras_concluidas_sem_pasta_no_fechamento))})')
            except Exception as e:
                print('\nsem acesso ao projeto ', i)
                print(resposta)
                print(resposta_pasta)
                traceback.print_exc()

            x += 1

        data_frame = pd.DataFrame(projetos_pendente_asbuilt, columns=['UNIDADE', 'PROJETO', 'TÍTULO', 'VALOR DO PROJETO', 'DATA DE ENERGIZAÇÃO', 'SUPERVISOR', 'MUNICÍPIO'])
        data_frame['PROJETO'] = data_frame['PROJETO'].drop_duplicates()
        data_frame = data_frame.dropna()
        pd.set_option('display.max_rows', None)

        espelho_CCM = self.le_planilha_google(configs.espelho_CCM, "Base de dados (Espelho)", 'B2:Y')
        espelho_CCM['PROJETO'] = pd.to_numeric(espelho_CCM['PROJETO'].str.replace('B-', ''), errors='coerce')
        espelho_CCM = espelho_CCM.dropna()

        data_frame['SUPERVISOR'] = data_frame['SUPERVISOR'].fillna(data_frame['PROJETO'].map(espelho_CCM.set_index('PROJETO')['Supervisor']))
        data_frame.set_option('display.max_columns', None)
        print(data_frame)

        while True:
            try:
                #dados_list = [data_frame.columns.values.tolist()] + data_frame.values.tolist()
                dados_list = data_frame.values.tolist()

                sh = self.GS_SERVICE.open_by_key('1GQ5pLG2DddGrEuRJILe-3g_Rwzhg-82EkVFZnX1_we4')
                pastas_pendentes = sh.worksheet('pastas pendentes')

                pastas_pendentes.clear()
                pastas_pendentes.update(values=dados_list, range_name='A1')
                sh.worksheet('data atualização').update(range_name='A1', values=[[datetime.now(self.br_tz).strftime("%d/%m/%Y %H:%M")]])
                break
            except Exception as e:
                print(e)
                sleep(30)


    # V5
    def consulta_projeto(self, projeto):
        datazps09 = ''
        estagio_hektor = ''
        projetoid = ''
        r = ''
        projeto = str(projeto)#.strip()

        body = {
            'id':projeto
        }

        r = self.fazer_requisicao(self.url_geo, body)
        
        if r['Content'] != None:
            if r['Content']['GseProjeto'] != None:
                estagio_hektor = r['Content']['GseProjeto']['Status']['Nome']
                
            if r['Content']['ProjetoId'] != None:
                projetoid = r['Content']['ProjetoId']
                
            if r['Content']['DtZps09'] != None:
                datazps09 = datetime.fromisoformat(r['Content']['DtZps09']).date().strftime("%d/%m/%Y")
        elif r['IsUnauthorized']:
            print(r)
            raise Exception('Cookie inválido! Não autorizado')
        
        return estagio_hektor, projetoid, datazps09

    def consulta_pasta(self, projetoid):
        #print(cookie, '\n', gxsessao, '\n', useragent)
        status_pasta = ''
        r = ''
        envio = 0

        body = {'ProjetoId': projetoid}

        r = self.fazer_requisicao(self.url_pasta, body)
        
        if r['Content'] != None:
            if r['Content']['Envios'] != None:
                for i in r['Content']['Envios']:
                    if i['EmpresaId']==70:
                        if i['Ultimo'] != None:
                            envio = i['Ultimo']['HistoricoStatusId']
                        break
                status_pasta = self.statuspastaid.get(envio,envio)
        elif r['IsUnauthorized']:
            print(r)
            raise Exception('Cookie inválido! Não autorizado')
        
        return status_pasta

    def consulta_termogeo(self, projetoid):
        #print(cookie, '\n', gxsessao, '\n', useragent)
        termo = ''
        r = ''

        body = {
            'ProjetoId': projetoid,
            'Paginacao': {'Pagina': 1, 'TotalPorPagina': 100}
        }

        r = self.fazer_requisicao(self.url_termo_geo, body)
        
        if r['Content'] != None:
            if r['Content']['Items'] != []:
                termo = r['Content']['Items'][0]['HistoricoStatus']['Nome']
        elif r['IsUnauthorized']:
            print(r)
            raise Exception('Cookie inválido! Não autorizado')
        
        return termo

    def consulta_encerramento(self, projetoid):
        status_encerramento = ''
        r = ''

        body = {'ProjetoId': projetoid}

        r = self.fazer_requisicao(self.url_encerramento, body)
        
        if r['Content'] != None:
            if r['Content']['Item'] != None:
                status_encerramento = r['Content']['Item']['HistoricoAtual']['Status']
        elif r['IsUnauthorized']:
            print(r)
            raise Exception('Cookie inválido! Não autorizado')
        
        return status_encerramento

    def atualiza_pasta_v5(self, aba):
        sh = self.GS_SERVICE.open_by_key(configs.id_planilha_postagemV5)
        valores = [[],[],[],[]]
        hektor = ''

        print('Atualizando V5')

        sheet = sh.worksheet(aba).get_all_values()
        sheet = pd.DataFrame(sheet, columns = sheet.pop(0))
        tamanho = sheet.shape[0]

        for i,j in enumerate(sheet['PROJETO']):
            
            if j=="":
                valores[0].append([''])
                valores[1].append([''])
                valores[2].append([''])
                hektor = ''
                status_pasta = ''
                status_encerramento = ''
            else:
                try:
                    hektor, projetoid, datazps09 = self.consulta_projeto(j)
                    if (hektor, projetoid, datazps09) == ('','',''):
                        hektor=''
                    sleep(1)
                    status_pasta = self.consulta_pasta(projetoid)
                    sleep(1)
                    status_encerramento = self.consulta_encerramento(projetoid)
                    
                    valores[0].append([hektor])
                    valores[1].append([status_pasta])
                    valores[2].append([status_encerramento])
                except Exception as e:
                    print(f'Erro no projeto {j}')
                    traceback.print_exc()
                    raise e
            
            print(f'{str(i+1)}/{str(tamanho)} - {j}')
            print(f'hektor: {hektor}')
            print(f'status_pasta: {status_pasta}')
            print(f'status_encerramento: {status_encerramento}')
            print('-------------------------------------------')

        sh.worksheet(aba).update(range_name="K2:K", values=valores[1])
        sh.worksheet(aba).update(range_name="AP2:AP", values=valores[0])
        sh.worksheet(aba).update(range_name="BG2:BG", values=valores[2])

        print('Pastas atualizadas!')



    # Envio de Pastas
    def hora_atual(self):
        data_e_hora_atuais = datetime.now()
        data_e_hora_em_texto = data_e_hora_atuais.strftime('%d/%m/%Y %H:%M')
        return data_e_hora_em_texto

    def consulta_projeto_pasta(self, projeto):
        datazps09 = ''
        try:
            r = self.geoex.consultar_projeto(projeto)
        except Exception as e:
            print("erro inesperado ao consultar projeto", projeto, r)
            raise e
        
        if r['sucess']:
            r = r['data']
            if r['DtZps09']!=None:
                datazps09 = datetime.fromisoformat(r['DtZps09']).date().strftime("%d/%m/%Y")
        else:
            if r['status_code'] == 400:
                return 'SEM ACESSO','SEM ACESSO','SEM ACESSO'
            raise Exception(
                    f"""
                    Falha ao consultar {projeto}.
                    Statuscode: {r['status_code']}
                    Message: {r['data']}
                    """
                )
            
        
        #ANALISTA	STATUS PASTA
        data_pasta, status_pasta = '', ''
        sleep(1)
        try:
            r = self.geoex.consultar_envio_de_pasta(r['ProjetoId'])
        except Exception as e:
            print("erro inesperado ao consultar projeto", projeto, r)
            raise e
        
        if r['sucess']:
            r = r['data']
            if r['Envios']!=None:
                for i in r['Envios']:
                    if i['Empresa']=='SIRTEC/SINO':
                        if i['Ultimo']!=None:
                            status_pasta = self.statuspastaid.get(i['HistoricoStatus'],i['HistoricoStatus'])
                        else:
                            status_pasta = 'PENDENTE'
                        
                        if status_pasta in ['PENDENTE', 'CANCELADO', 'REJEITADO']:
                            data_pasta = datetime.today().date().strftime("%d/%m/%Y")
                        elif status_pasta == 'CRIADO':
                            data_pasta = datetime.fromisoformat(i['Ultimo']['Data']).date().strftime("%d/%m/%Y")
                        elif status_pasta == 'VALIDADO':
                            data_pasta = datetime.fromisoformat(i['Ultimo']['DataValidacao']).date().strftime("%d/%m/%Y")
                        elif status_pasta in ['ACEITO', 'ACEITO COM RESTRIÇÕES']:
                            data_pasta = datetime.fromisoformat(i['Ultimo']['DataBaixa']).date().strftime("%d/%m/%Y")
            else:
                data_pasta = datetime.today().date().strftime("%d/%m/%Y")
                status_pasta = 'PENDENTE'
        else:
            raise Exception(
                    f"""
                    Falha ao consultar {projeto}.
                    Statuscode: {r['status_code']}
                    Message: {r['data']}
                    """
                )
                
        return datazps09, data_pasta, status_pasta

    def atualiza_pasta(self):
        planilha = 'https://docs.google.com/spreadsheets/d/1p5hP6cXqZ67jUksUivGhiCd_F_wxP5PMpZIW8jxzHPA/edit?gid=445257240#gid=445257240'
        sh = self.gs.le_planilha(planilha, 'PRAZO ENVIO', intervalo='A:H')
        projetos = list(sh["PROJETO"])
        total = len(projetos)
        print(self.hora_atual() + ': Atualizando Projetos')
        valores = []

        for idx, projeto in enumerate(projetos, start=1):
            if projeto != '' and projeto[0] == 'B':
                print(f'Atualizando {idx}/{total} - {projeto}')
                valores.append((projeto,) + self.consulta_projeto_pasta(projeto))
            else:
                print(f'Pulando {idx}/{total} - linha vazia')
                valores.append(['','','',''])


        self.gs.escreve_planilha(planilha, 'Status de Pastas - BOT', pd.DataFrame(valores, columns=['PROJETO','DATA ZPS09', 'DATA PASTA', 'STATUS PASTA']), range='A2:D', input_option='USER_ENTERED')
        print(self.hora_atual() + ': Pastas atualizadas!')
        #print(valores)
