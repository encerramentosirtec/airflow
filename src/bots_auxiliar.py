import pandas as pd
from gspread import authorize, utils
from oauth2client.service_account import ServiceAccountCredentials

import os
from src.geoex import Geoex
from src.bots_ccm import Bots

import smtplib
from os.path import basename
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.multipart import MIMEMultipart
from email.mime.application import MIMEApplication

import seaborn as sns
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.ticker as mtick
from babel.numbers import format_currency

class Bots_aux():
    def __init__(self):
        self.planilha_gpm = '1_M4Ae3TkkbTl9XYrKviuYkzl0TStvfbEWox_oR9DpP0'
        self.planilha_rejeicoes = '1RhXWgyRZwEZHU-RAin0l3mrjyGJ5uvvxB86vkD8aIr8'

        self.PATH = os.getenv('AIRFLOW_HOME')
        self.cred_path = 'jimmy.json'
        self.cookie_path = 'cookie_ccm.json'
        self.geoex = Geoex(self.cookie_path)
        self.bot = Bots(self.cookie_path, self.cred_path)
        self.gs = self.bot.GS_SERVICE

        self.id = 'MmdCNEV0dVRxYVRnUGdMZSFhVCN0SnV0SjNWZWBUZS1UcXZhJ3QnNEpnbGcjdHF0djR1cVRKZ1AyZ2RhcXZ0Z1BnWExITFhMQkFWciAiIGhITDtZcmdsZ2RhcXZ0SmdQRGxnZGFxdnRWJ2dQRGxnOGVhJ3QnNEpnUC9ERGlsRFcmbHwobER8JmxdRERsRG9ubER8bmxEb29sXURdbF1Eb2xEXWlsXURXbERdfGxEfDdsXUR8bERvfGxEXURsRFdvbF1XbEREKGxvKGxXbmxEfGlsXWxEbzdsRHwobFdsRG5sRHxEbER8bERvbERXbEREbERdbERdJmxpbER8XWwmbER8b2xEb11sN2wobF1ubF03bF0obERvKGxEXW9sRCZsRF1ubF1EbEREJmxEb1dsXXxsfGxvbF1dbERvRGxEfFdsRChsRF0obEQ3bERdN2xEXVdsRGxdb2xEaWxEXV1mbGdJYU9USkhLcXRKZ1AvZmxnTHZPcTRKdEpnUC8obmZsZ0B1dHV9SmdQL3xdZmxnSVQnVEpnUC10RUo0PmxnZGFFdXFUSmdQLzJnWFR2NGdQZ0BJckk4QGdsZ150RVRxZ1BnQkx7TFZJcl9IZz5sMmdYVHY0Z1BnSUhfSEBnbGdedEVUcWdQZ1jDg0hnPmwyZ1hUdjRnUGdJViNIIF9MIEhZQnJnbGdedEVUcWdQZ2c+bDJnWFR2NGdQZyNMQsONSF9IICIgSVYjSGdsZ150RVRxZ1BnQEg7VmhWSXLDh8ODSGc+bDJnWFR2NGdQZyNMQsONSF9IZ2xnXnRFVHFnUGdnPmwyZ1hUdjRnUGc4WFZfcl9MZ2xnXnRFVHFnUGdnPmwyZ1hUdjRnUGdMWyNCTEByZ2xnXnRFVHFnUGdAVkJJTGgzQFZYSGc+bDJnWFR2NGdQZ0xJVis4TElyZ2xnXnRFVHFnUGdnPmwyZ1hUdjRnUGc4QDjDgUJWSGdsZ150RVRxZ1BnZz5mbGdbVCd9RVRnUGdMZWA0cXF0djRldVRoVGV1dEthRWdsZ1hUdjRnUGdyYFR2TyAiIExlIWFUICc0IE90SnV0SiAiIFZlYFRlLVRxdmEndCc0SmdsZ0lhT1RnUGdZdGFDdHFnbGdMQ3U0ZUp0VGdQZ3NgSiFnPg=='
        self.setores = {
                        ('BLINDAGEM', 'REDE BLINDADA'): 'CCM',
                        ('COMERCIAL', 'COBRANÇA'): 'STC',
                        ('COMERCIAL', 'NOVAS LIGAÇÕES'): 'STC',
                        ('COMERCIAL', 'PARECER TÉCNICO 023'): 'STC',
                        ('COMERCIAL', 'PERDAS'): 'STC',
                        ('EQUIPAMENTOS', 'BANCO DE CAPACITOR'): 'CCM',
                        ('EQUIPAMENTOS', 'CAPACITOR'): 'CCM',
                        ('EQUIPAMENTOS', 'CHAVE'): 'CCM',
                        ('EQUIPAMENTOS', 'EQUIPAMENTO'): 'CCM',
                        ('EQUIPAMENTOS', 'REGULADOR'): 'CCM',
                        ('EQUIPAMENTOS', 'RELIGADOR'): 'CCM',
                        ('EQUIPAMENTOS', 'SENSORES'): 'CCM',
                        ('INTERVENÇÃO', 'CORRETIVA'): 'MANUT',
                        ('INTERVENÇÃO', 'CORRETIVA - PASSIVO'): 'MANUT',
                        ('INTERVENÇÃO', 'PREVENTIVA'): 'MANUT',
                        ('INTERVENÇÃO', 'PREVENTIVA - PASSIVO'): 'MANUT',
                        ('INTERVENÇÃO', 'RAMAL CORRETIVA'): 'MANUT',
                        ('INTERVENÇÃO', 'SOLAR'): 'STC',
                        ('LUZ PARA TODOS', 'ALTERAÇÃO DE CARGA'): 'CCM',
                        ('LUZ PARA TODOS', 'EXPANSÃO RURAL'): 'CCM',
                        ('LUZ PARA TODOS', 'EXPANSÃO URBANA'): 'CCM',
                        ('LUZ PARA TODOS', 'KIT LPT'): 'CCM',
                        ('LUZ PARA TODOS', 'MEDIDOR LPT'): 'CCM',
                        ('LUZ PARA TODOS', 'NOVAS LIGAÇÕES'): 'CCM',
                        ('LUZ PARA TODOS', 'REDES LPT'): 'CCM',
                        ('MANUT. SUBTRANSMISSÃO', 'MANUTENÇÃO SUB'): 'MANUT',
                        ('ODS', 'ILUMINAÇÃO PUBLICA'): 'STC',
                        ('ODS', 'INSTALAÇÃO PROVISÓRIA'): 'STC',
                        ('ODS', 'KIT COMERCIAL'): 'STC',
                        ('ODS', 'OUTROS'): 'STC',
                        ('ODS', 'PADRÃO LPT'): 'STC',
                        ('ODS', 'RELOCAÇÃO'): 'STC',
                        ('OUTROS', 'OUTROS'): 'OUTROS',
                        ('REDE ESPECIAL', 'INCORPORAÇÃO'): 'CCM',
                        ('REDE ESPECIAL', 'SUBTERRÂNEO'): 'CCM',
                        ('REDES', 'ALIMENTADOR'): 'CCM',
                        ('REDES', 'ALTERAÇÃO DE CARGA'): 'CCM',
                        ('REDES', 'DISTRIBUIÇÃO'): 'CCM',
                        ('REDES', 'EXPANSÃO RURAL'): 'CCM',
                        ('REDES', 'EXPANSÃO URBANA'): 'CCM',
                        ('REDES', 'INTERLIGAÇÃO'): 'CCM',
                        ('REDES', 'MELHORAMENTO'): 'CCM',
                        ('REDES', 'MELHORAMENTO BT'): 'CCM',
                        ('REDES', 'NÍVEL DE TENSÃO'): 'CCM',
                        ('REDES', 'RELOCAÇÃO'): 'CCM',
                        ('REDES', 'SOLAR'): 'STC'
                        }
        self.projetos_solar = [
                    "B-1160585",
                    "B-1164163",
                    "B-1164268",
                    "B-1171923",
                    "B-1171933",
                    "B-1175485",
                    "B-1175543",
                    "B-1176528",
                    "B-1183486",
                    "B-1184574",
                    "B-1184714",
                    "B-1158437"
                    ]

    # Sequencia de dias com pendencia por equipe
    def atualiza_sequencia(self):
        print('Buscando equipes')
        sh = self.gs.open_by_key(self.planilha_gpm)
        ws1 = sh.worksheet('Página1')
        ws3 = sh.worksheet('Página3')
        sheet = ws1.get_all_values(value_render_option=utils.ValueRenderOption.unformatted)
        sheet = pd.DataFrame(sheet, columns = sheet.pop(0))
        sheet = sheet[sheet['Pendências']==1]
        sheet['Data Serviço'] = pd.to_datetime('1899-12-30') + pd.to_timedelta(sheet['Data Serviço'], unit='D')
        sheet = sheet.drop_duplicates(subset=['Equipe', 'Data Serviço'])
        sheet.sort_values(['Equipe', 'Data Serviço'], ascending=[True, False], inplace=True)
        sheet.reset_index(drop=True, inplace=True)

        anterior = ''
        sequencia = {}
        repetidos = []

        print('Verificando sequencias de pendências')
        for i, row in sheet.iterrows():
            if i == 0:
                sequencia[row['Equipe']] = [1, row['Tipo'], row['Operação'], row['Gerência'], row['Ultima pendência']]
            elif row['Equipe'] in repetidos:
                pass
            elif row['Equipe'] == anterior['Equipe']:
                datadif = anterior['Data Serviço'] - row['Data Serviço']
                dias = datadif.days
                dia_semana = anterior['Data Serviço'].weekday() #Segunda=0, Domingo=6
                if dias == 1 or (dias ==2 and dia_semana == 0):
                    sequencia[row['Equipe']][0] += 1
                else:
                    repetidos.append(row['Equipe'])
                    pass
            else:
                sequencia[row['Equipe']] = [1, row['Tipo'], row['Operação'], row['Gerência'], row['Ultima pendência']]
            anterior = row
            
        lista = [[key]+value for key, value in sequencia.items()]

        print('Atualizando planilha')
        sh.values_clear("Página3!E2:I")
        ws3.update(range_name='E2', values=lista, value_input_option='USER_ENTERED')
    

    # Email rejeicoes
    def relatorio(self):
        download = self.geoex.baixar_relatorio(self.id, 'rejeicoes', 'downloads')
        
        if download['sucess']:
            print('Download concluido.')
        else:
            print(download)
            raise Exception(
                f'''
                Falha ao baixar csv.
                Statuscode: { download['status_code'] }
                Message: { download['data'] }
                '''
            )
    
    def obter_setor(self, row):
        return self.setores.get((row['GRUPO'], row['SUB_GRUPO']), 'N/A')  # 'N/A' se não existir
    
    def plotar_grafico(self, df, x, y, z):
        sns.set_theme(style="whitegrid", palette="pastel")
        plt.figure(figsize=(15, 4))
        
        plt.subplot(1, 2, 2)
        ax1 = sns.barplot(data=df, x=x, y=y, hue=x, palette='gist_ncar_r')
        plt.title('Pendências por Setor', fontsize=16)
        plt.xlabel('Setor', fontsize=14)
        plt.ylabel('Quantidade', fontsize=14)
        
        plt.subplot(1, 2, 1)
        ax2 = sns.barplot(data=df, x=x, y=z, hue=x, palette='gist_ncar_r')
        plt.title('Pendências por Setor', fontsize=16)
        plt.xlabel('Setor', fontsize=14)
        plt.ylabel('Valor', fontsize=14)
        ax2.yaxis.set_major_formatter(mtick.StrMethodFormatter('R${x:,.2f}'))
        
        for container in ax1.containers:
            ax1.bar_label(container)
            
        for p in ax2.patches:
            ax2.annotate(f'R${p.get_height():,.2f}', 
                (p.get_x() + p.get_width() / 2., p.get_height()),
                ha='center', va='top', 
                xytext=(0, 10), 
                textcoords='offset points',
                fontsize=10)
            
        plt.savefig(os.path.join(self.PATH,'downloads/grafico.png'), dpi=300, bbox_inches='tight')
        plt.close() 
    
    def tratamento(self):
        #lendo planilha online e arquivos csv
        print('lendo projetos')
        unidades = self.bot.le_planilha_google(self.planilha_rejeicoes, 'Projeto/UTD')
        unidades['VALOR'] = pd.to_numeric(unidades['VALOR'], errors='coerce')
        #print(unidades['VALOR'])
        df = pd.read_csv(os.path.join(self.PATH,'downloads/rejeicoes.csv'), encoding='ISO-8859-1', sep=';', thousands='.', decimal=',', low_memory=False)
        antigos = pd.read_csv(os.path.join(self.PATH,'downloads/anteriores.csv'), encoding='ISO-8859-1', sep=';', low_memory=False)
        projetos_antigos = antigos['PROJETO'].to_list()
        
        #preenchendo os projetos sem unidade com a unidade informada na planilha
        mapa = dict(zip(unidades['Projeto'], unidades['UTD']))
        valores = dict(zip(unidades['Projeto'], unidades['VALOR']))
        localidade = df['PROJETO'].map(mapa)

        df = df[~df['PROJETO'].isin(self.projetos_solar)]
        
        df['VALOR'] = df['PROJETO'].map(valores)
        df['LOCALIDADE'].fillna(localidade, inplace=True)
        df['VALOR'].fillna(0, inplace=True)
        
        #definindo o setor de cada projeto
        df['SETOR'] = df.apply(self.obter_setor, axis=1)
        #filtra os projetos unitizados
        df = df[df['PROJETO'].str.startswith('B')]
        df['DATA_PENDENCIA'] = pd.to_datetime(df['DATA_PENDENCIA'], format='%d/%m/%Y')
        data_filtro = pd.to_datetime('01/05/2025', format='%d/%m/%Y')
        df = df[df['DATA_PENDENCIA'] >= data_filtro]
        
        #retira os projetos já enviados da relação
        projetos = df[['PROJETO','SETOR','VALOR']].drop_duplicates()
        projetos['REPETICOES'] = 1
        def incrementar_repeticao(row):
            if row['PROJETO'] in projetos_antigos:
                row['REPETICOES'] = antigos.loc[antigos['PROJETO']==row['PROJETO'],'REPETICOES'].values[0]+1
            else:
                row['REPETICOES'] = 1
            return row
        projetos = projetos.apply(incrementar_repeticao, axis=1)
        
        repeticoes = dict(zip(projetos['PROJETO'], projetos['REPETICOES']))
        df['REPETICOES'] = df['PROJETO'].map(repeticoes)
        
        #mantem na relação de projeto enviados apenas os que continuam rejeitados
        df.sort_values(by=['SETOR','LOCALIDADE','DATA_PENDENCIA'], inplace=True) #ordena por setor, utd e data
        df = df.reset_index(drop=True).fillna('')

        df.columns = df.columns.str.replace('_', ' ') #retira os underline parar deixar os titulos mais legíveis
        def join_with_empty(vals):
            return '; '.join(sorted(set(v for v in vals if pd.notna(v) and v != '')))

        #agrupa os projetos e ordena por setor e data
        df2 = df.groupby(['SETOR','LOCALIDADE','USUARIO SOLICITACAO','PROJETO','DATA PENDENCIA','PROTOCOLO','VALOR','REPETICOES'])['OBSERVACAO'].agg(join_with_empty).reset_index()
        df2.sort_values(by=['SETOR', 'DATA PENDENCIA'], inplace=True)
        df2['DATA PENDENCIA'] = df2['DATA PENDENCIA'].dt.strftime('%d/%m/%Y')
        df2['REPETICOES'] = df2['REPETICOES'].astype(int)
        
        #atualiza relação de projetos já enviados
        projetos.to_csv(os.path.join(self.PATH,'downloads/anteriores.csv'), index=False, sep=';')#, thousands='.', decimal=',')
        
        #df2['VALOR TOTAL'] = 'R$ ' + df2['VALOR'].round(2).astype(str)
        df2['VALOR TOTAL'] = df2['VALOR'].apply(
            lambda x: format_currency(x, 'BRL', locale='pt_BR')
        )
        
        dados = projetos.groupby('SETOR').agg(
            CONTAGEM=('PROJETO', 'count'),
            VALOR=('VALOR', 'sum')
        )
        self.plotar_grafico(dados, 'SETOR', 'CONTAGEM', 'VALOR')
        print(f'{df2.shape[0]} novas rejeições')
        dados = dados.reset_index()
        print(dados)
        
        def colorir_notas(val):
            if val >= 3:
                return 'background-color: red; color: white'
            elif val == 2:
                return 'background-color: #FFC107; color: black'
            else:
                return 'background-color: green; color: white'

        df3 = df2[['SETOR','LOCALIDADE','USUARIO SOLICITACAO','PROJETO','VALOR TOTAL','DATA PENDENCIA','REPETICOES','OBSERVACAO']]
        df3 = df3.style.applymap(colorir_notas, subset=['REPETICOES'])
        
        if projetos[~projetos['PROJETO'].isin(antigos['PROJETO'])].shape[0]>0:
            print('atualizando historico')
            historico = pd.read_csv(os.path.join(self.PATH,'downloads/historico.csv'), encoding='ISO-8859-1', sep=';', low_memory=False)
            dados['DATA_REGISTRO'] = datetime.now()
            novohist = pd.concat([historico,dados])
            novohist.to_csv(os.path.join(self.PATH,'downloads/historico.csv'), index=False, sep=';')
            
        if projetos.shape[0] != 0:
            return df3.hide(axis='index').to_html(), projetos[~projetos['PROJETO'].isin(antigos['PROJETO'])].shape[0], projetos.shape[0]
        else:
            return 0,0,0

    def enviaEmail(self):
        tabela = self.tratamento()
        if tabela[0] == 0:
            return 'Sem novas rejeições'
        
        # configuração
        port = 465
        smtp_server = "email-ssl.com.br"
        login = "encerramento.obraseservicos@sirtec.com.br"  # Your login
        password = "cb!Verdadef6"  # Your password
        sender_email = "encerramento.obraseservicos@sirtec.com.br"
        
        '''port = 465
        smtp_server = "smtp.gmail.com"
        login = "sirtec.heli@gmail.com"  # Your login
        password = "zizh zwik bkhq oaes"  # Your password
        sender_email = "sirtec.heli@gmail.com"'''

        receiver_emails_test = ["heli.silva@sirtec.com.br"]
        receiver_emails = [
            "claudinei.alves@sirtec.com.br",
            'adriele.jesus@sirtec.com.br',
            'allan.alves@sirtec.com.br',
            'anderson.almeida@sirtec.com.br',
            'anthony.almas@sirtec.com.br',
            'beatriz.goncalves@sirtec.com.br',
            'brenda.moreira@sirtec.com.br',
            'clara.santos@sirtec.com.br',
            'claudio.sousa@sirtec.com.br',
            'crelson.santos@sirtec.com.br',
            'cristiane.neves@sirtec.com.br',
            'elisangela.barreto@sirtec.com.br',
            'evelyn.pereira@sirtec.com.br',
            'fabricio.santos@sirtec.com.br',
            'flavio.oliveira@sirtec.com.br',
            'francielle.silva@sirtec.com.br',
            'gabriel.flores@sirtec.com.br',
            'gabriel.oliveira@sirtec.com.br',
            'gabriel.brito@sirtec.com.br',
            'gessica.pereira@sirtec.com.br',
            'heli.silva@sirtec.com.br',
            'hugo.viana@sirtec.com.br',
            'janaina.reis@sirtec.com.br',
            'joao.oliveira@sirtec.com.br',
            'joao.pereira@sirtec.com.br',
            'jose.asterio@sirtec.com.br',
            'josimeire.santana@sirtec.com.br',
            'juliana.bomfim@sirtec.com.br',
            'lais.moreira@sirtec.com.br',
            'larissa.sousa@sirtec.com.br',
            'lucas.brezolin@sirtec.com.br',
            'luiz.silva@sirtec.com.br',
            'luiz.cardoso@sirtec.com.br',
            'maik.silva@sirtec.com.br',
            'maria.almeida@sirtec.com.br',
            'leticia.oliveira@sirtec.com.br',
            'michelle.castro@sirtec.com.br',
            'natalia.ramos@sirtec.com.br',
            'ramon.mendes@sirtec.com.br',
            'romeu.oliveira@sirtec.com.br',
            'mateus.coutinho@sirtec.com.br',
            'thalia.rocha@sirtec.com.br',
            'stefani.costa@sirtec.com.br',
            'wanderson.silva@sirtec.com.br',
            'uennede.cruz@sirtec.com.br'
        ]

        # HTML content with an image embedded
        html = f"""<html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
            <link rel="stylesheet" href="styles.css">
            <title>Relatório de Atendimentos</title>
            <style>
                table {{
                    border-collapse: collapse;
                    min-width: 150px;
                    margin: 0px;
                }}
                th, td {{
                    border: 1px solid #ddd;
                    padding: 8px;
                }}
                th {{
                    background-color: #f2f2f2;
                }}
                td {{
                    white-space: nowrap;
                }}
                td:last-child {{
                    white-space: normal;
                }}
                td:nth-child(2) {{
                    white-space: normal;
                    word-wrap: break-word;
                }}
            </style>
        </head>
        <body>
            <p>Prezados(as),
            <p>Segue relatório periódico contendo a lista de projetos reprovados desde a ultima atualização. Foram {tabela[1]} novas reprovações de um total de {tabela[2]}.
            <p>Em anexo segue relatório em CSV com todos dos projetos rejeitados<br><br>
            <p><img src="cid:grafico" width="50%"></p><br>
            {tabela[0]}
        </body>
        </html>"""

        # Create a multipart message and set headers
        message = MIMEMultipart()
        message["From"] = sender_email
        message["To"] = ", ".join(receiver_emails_test)
        #message["Bcc"] = "heli.silva@sirtec.com.br"
        message["Subject"] = "Relatório de rejeições de pastas"

        # Attach the HTML part
        message.attach(MIMEText(html, "html"))
        
        with open(os.path.join(self.PATH,'downloads/grafico.png'), 'rb') as img:
                msg_img = MIMEImage(img.read(), name=os.path.basename(os.path.join(self.PATH,'downloads/grafico.png')))
                msg_img.add_header('Content-ID', f'<grafico>')
                message.attach(msg_img)

        with open(os.path.join(self.PATH,'downloads/rejeicoes.csv'), "rb") as fil:
            part = MIMEApplication(
                fil.read(),
                Name=basename(os.path.join(self.PATH,'downloads/rejeicoes.csv'))
            )
        # After the file is closed
        part['Content-Disposition'] = 'attachment; filename="%s"' % basename(os.path.join(self.PATH,'downloads/rejeicoes.csv'))
        message.attach(part)
        
        print('envia email')
        # Send the email
        with smtplib.SMTP_SSL(smtp_server, port) as server:
            #server.starttls()
            server.login(login, password)
            server.sendmail(sender_email, receiver_emails_test, message.as_string())

        print('Sent')
