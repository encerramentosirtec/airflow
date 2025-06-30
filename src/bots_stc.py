import os
import json
import gspread
import datetime
import pandas as pd
from time import sleep
from src.geoex import Geoex
from pendulum import timezone

class Bots:

    def __init__(self, cookie_path='assets/auth_geoex/cookie_ccm.json', cred_path='assets/auth_google/jimmy.json'):
        self.PATH = os.getenv('AIRFLOW_HOME')
        
        with open(os.path.join(self.PATH, cookie_path), 'r') as f:
            self.cookie = json.load(f)
            
        self.geoex = Geoex('cookie_ccm.json')
        self.cred_path = cred_path
        self.GS_SERVICE = gspread.service_account(filename=os.path.join(self.PATH, cred_path))
        self.br_tz = timezone("Brazil/East")

        self.url_geo = 'Cadastro/ConsultarProjeto/Item'
        self.url_pasta = 'ConsultarProjeto/EnvioPasta/Itens'
        self.planilha_solar = '12NMUcBYJQTedvejCpemui4iAT7s6u4U8wtGk4MFgkCs'
        
        self.reservas_ids = {
            1: 'CRIADO',
            6 : 'CANCELADO',
            30 : 'ACEITO',
            31 : 'ACEITO COM RESTRIÇÕES',
            32 : 'REJEITADO',
            35 : 'VALIDADO'
        }
        self.statuspastaid = {
            1 : 'CRIADO',
            6 : 'CANCELADO',
            30 : 'ACEITO',
            31 : 'ACEITO COM RESTRIÇÕES',
            32 : 'REJEITADO',
            35 : 'VALIDADO'
        }
        
        self.status_hro = {
            '':                     'A - FALTA ENVIAR',
            'CRIADO':               'B - CRIADO',
            'ENVIADO':              'C - ENVIADO',
            'ENVIO CANCELADO':      'D - ENVIO CANCELADO',
            'VALIDANDO':            'E - VALIDANDO',
            'VALIDADO':             'F - VALIDADO',
            'VALIDAÇÃO CANCELADA':  'G - VALIDAÇÃO CANCELADA',
            'EM ANÁLISE':           'H - EM ANÁLISE',
            'ANALISADO':            'I - ANALISADO',
            'ANÁLISE CANCELADA':    'J - ANÁLISE CANCELADA',
            'REJEITADO':            'K - REJEITADO',
            'ACEITO':               'L - ACEITO',
            'EXPURGADO':            'M - EXPURGADO',
            'CANCELADO':            'N - CANCELADO',
        }
        self.meses = {
            'JAN': 'JANEIRO',
            'FEV': 'FEVEREIRO',
            'MAR': 'MARÇO',
            'ABR': 'ABRIL',
            'MAI': 'MAIO',
            'JUN': 'JUNHO',
            'JUL': 'JULHO',
            'AGO': 'AGOSTO',
            'SET': 'SETEMBRO',
            'OUT': 'OUTUBRO',
            'NOV': 'NOVEMBRO',
            'DEZ': 'DEZEMBRO',
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


    # Pastas de Solar
    def consulta_projeto(self, projeto):
        datazps09, investimento, tipologia, contrato = '', '', '', ''
        try:
            r = self.geoex.run("POST", self.url_geo, json={'id': projeto}).json()
        except Exception as e:
            print(projeto, r.content, str(r.status_code), str(r.reason))
            raise e
            
        try:
            body = {'ProjetoId': r['Content']['ProjetoId']}
        except Exception as e:
            print(projeto, r['Message'], r['Content'], r)
            raise e
        
        if r['Content']['DtZps09'] != None:
            if r['Content']['DtZps09']!=None:
                datazps09 = datetime.datetime.fromisoformat(r['Content']['DtZps09']).date().strftime("%d/%m/%Y")
            if r['Content']['PosicaoInvestimento']!=None:
                investimento = r['Content']['PosicaoInvestimento']
            if r['Content']['ArquivoTipologia']!=None:
                tipologia = r['Content']['ArquivoTipologia']['Nome']
            if len(r['Content']['Contratos'])!=0:
                contrato = r['Content']['Contratos'][0]['Numero']

        
        #ANALISTA	STATUS PASTA
        data_pasta, analista, data_validacao, status_pasta = '', '', '', ''
        sleep(1)
        try:
            r = self.geoex.run("POST", self.url_pasta, json=body).json()
        except Exception as e:
            print(projeto, r.content, str(r.status_code), str(r.reason))
            raise e
        
        try:
            if len(r['Content']['Envios'])!=0:
                for i in r['Content']['Envios']:
                    if i['Empresa']=='SIRTEC/SINO' and i['Ultimo']!=None:
                        data_pasta = datetime.datetime.fromisoformat(i['Ultimo']['Data']).date().strftime("%d/%m/%Y")
                        analista = i['Ultimo']['Usuario']['NomeResumo']
                        if i['Ultimo']['DataValidacao']!=None:
                            data_validacao = datetime.datetime.fromisoformat(i['Ultimo']['DataValidacao']).date().strftime("%d/%m/%Y")
                        status_pasta = self.statuspastaid.get(i['Ultimo']['HistoricoStatusId'],i['Ultimo']['HistoricoStatusId'])
                        break
        except Exception as e:
            print(projeto, r['Message'], r['Content'])
            raise e
                
        return data_pasta, analista, data_validacao, status_pasta, datazps09, investimento, tipologia, contrato
        
    def atualiza_solar(self):
        sh = self.GS_SERVICE.open_by_url(self.planilha_solar)
        ws = sh.worksheet('Postagem de projetos')
        sheet = ws.get_all_values()
        sheet = pd.DataFrame(sheet, columns = sheet.pop(0))
        sheet['PROJETO'] = sheet['PROJETO'].str.strip()
        sheet = sheet[sheet['PROJETO']!='']
        
        valores1 = []
        valores2 = []
        for i, projeto in enumerate(sheet['PROJETO']):
            status = self.consulta_projeto(projeto)
            valores1.append(status[0:2])
            valores2.append(status[3:])
            print(f'{i+1}/{sheet.shape[0]}: {projeto}, {status}')
        
        ws.update(range_name='I2', values=valores1, value_input_option='USER_ENTERED')
        ws.update(range_name='M2', values=valores2, value_input_option='USER_ENTERED')
    

    # Relatório HRO
    def relatorio(self):
        hros = '1o8byF41_AmcXFykW8IcyN7fZkUmRZpICiJWqlpbT82M'
        sh = self.GS_SERVICE.open_by_key(hros)
        infos = sh.worksheet('Infos').get_all_values()
        
        download = self.geoex.baixar_relatorio(infos[1][1], "sudoeste-oeste", 'downloads')
        download2 = self.geoex.baixar_relatorio(infos[1][2], "norte", 'downloads')
        
        if download['sucess']*download2['sucess']:
            print('Download concluido.')
        else:
            raise Exception(
                f"""
                Falha ao baixar csv.
                Statuscode: { download['status_code'] }
                Message: { download['data'] }
                Statuscode2: { download2['status_code'] }
                Message2: { download2['data'] }
                """
            )
            
    def tratamento(self):
        df1 = pd.read_csv(os.path.join(self.PATH, 'downloads/sudoeste-oeste.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',', low_memory=False)
        print('lendo sudoeste-oeste')
        df2 = pd.read_csv(os.path.join(self.PATH, 'downloads/norte.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',', low_memory=False)
        print('lendo norte')
        
        df = pd.concat([df1, df2])
        
        df['STATUS AJUSTADO'] = df['STATUS'].map(self.status_hro)
        print('ajustando status')
        df['CICLO'] = df['TITULO'].apply(lambda x: x.split(' / ')[4]).map(self.meses)

        hros = '1o8byF41_AmcXFykW8IcyN7fZkUmRZpICiJWqlpbT82M'
        sh = self.GS_SERVICE.open_by_key(hros)
        ws = sh.worksheet('BASE_GEOEX')
        ws.clear()
        
        while True:
            print('atualizando planilha')
            try:
                print('titulos')
                ws.update(range_name='A1', values=[df.columns.tolist()])
                print('valores')
                ws.update(range_name='A2', values=df.fillna("").values.tolist())
                break
            except Exception as e:
                print(e)
        
        print('data')
        date_now = datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=-3))).strftime('%d/%m/%Y, %H:%M:%S')
        sh.worksheet('Infos').update_acell('B1', date_now)
    