import os
import json
import logging
import telebot
import requests
import traceback
from datetime import datetime
#from airflow.sdk import Variable
from airflow.models import Variable
from hooks.geoex_hook import GeoexHook

def abre_json(arquivo):
    with open(arquivo) as dados:
        file = json.load(dados)
    return file

def escreve_json(arquivo, dicionario):
    with open(arquivo, "w") as outfile: 
        json.dump(dicionario, outfile)

class Bots:

    def __init__(self):
        self.PATH = os.getenv('AIRFLOW_HOME')

        #os.environ['NO_PROXY'] = '*'
        API_TOKEN = Variable.get("telegram_api_key")
        self.bot = telebot.TeleBot(API_TOKEN, threaded=False)
        #telebot.logger.setLevel(logging.DEBUG) # Exibe log detalhado

        self.cookie, self.gxsessao, self.gxbot = '', '', ''
        self.data = abre_json(os.path.join(self.PATH, 'assets/auth_geoex/cookie_heli.json'))
        self.data_bob = abre_json(os.path.join(self.PATH,'assets/auth_geoex/cookie_ccm.json'))
        self.data_hugo = abre_json(os.path.join(self.PATH,'assets/auth_geoex/cookie_hugo.json'))

    def testa_cookie(self, c='', g='', gb=''):
        url_geo = 'Cadastro/ConsultarProjeto/Item'
        if c=='' and g=='' and gb=='':
            return False
        id_projeto = ''
        r = ''
        projeto = 'B-1131975'

        cookie = {
            'cookie': c,
            'useragent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
            'gxsessao': g,
            'gxbot': gb
        }
        body = {
            'id': projeto
        }

        while True:
            try:
                r = GeoexHook(cookie).run('POST', url_geo, json = body)
                r = r.json()
                break
            except Exception as e:
                print('Não foi possível acessar a página do GEOEX.')
                print(r.content)
                return False

        if r['Content'] != None:
            id_projeto = r['Content']['ProjetoId']
        elif r['IsUnauthorized']:
            print('Cookie inválido! Não autorizado')
            return False

        if id_projeto=='':
            return False
        else:
            return True

    def push_cookie(self):
        data_new = abre_json('assets/auth_geoex/cookie_heli.json')
        
        Variable.set(key='cookie_manut', value=data_new['cookie'])
        Variable.set(key='gxsessao_manut', value=data_new['gxsessao'])
        Variable.set(key='gxbot_manut', value=data_new['gxbot'])
        if Variable.get("useragent_manut") == None:
            Variable.set(key='useragent_manut', value=data_new['useragent'])

    def get_cookie(self):
        cookie = Variable.get("cookie_manut")
        gxsessao = Variable.get("gxsessao_manut")
        gxbot = Variable.get("gxbot_manut")
        useragent = Variable.get("useragent_manut")
        #print(cookie, '\n', gxsessao, '\n', gxbot, '\n', useragent)
        return cookie, gxsessao, gxbot, useragent

    def trigger_dag(self, dag_id):
        token = abre_json(os.path.join(self.PATH,'access_token.json'))
        headers = {'Content-Type': 'application/json',
                   'Authorization': f'Bearer {token['access_token']}'
                    }
        
        response = requests.post(
            f"http://localhost:8080/api/v2/dags/{dag_id}/dagRuns",
            headers=headers,
            json={
                'logical_date': datetime.now().astimezone().isoformat()
            }
        )
        return response

    def run_bot(self):
        print("Iniciando bot")

        @self.bot.message_handler(commands=['help', 'start'])
        def send_welcome(message):
            self.bot.send_message(message.chat.id, """Bot para atualização das planilhas do Fechamento
            Comandos:
            /help - Ajuda sobre o funcionamento do bot 
            /cookie - Atualizar cookie e gxsessao
            /cookie_hugo - Atualizar cookie e gxsessao
            /xcom - Atualizar XCom com o cookie
            """)

        @self.bot.message_handler(commands=['xcom'])
        def xcom_start(message):
            r = self.trigger_dag('cookie-manut')
            self.bot.send_message(message.chat.id, f'{r.status_code} | {json.dumps(r.json(), indent=4, ensure_ascii=False)}')

        @self.bot.message_handler(commands=['cookie'])
        def send_cookie(message):
            msg = self.bot.send_message(message.chat.id, '''
            Atualizando informações de acesso ao Geoex.
            Insira o Cookie:
            ''')
            self.bot.register_next_step_handler(msg, ler_cookie)

        def ler_cookie(message):
            self.cookie = message.text
            msg = self.bot.send_message(message.chat.id, 'Insira Gxbot:')
            self.bot.register_next_step_handler(msg, ler_gxbot)

        def ler_gxbot(message):
            self.gxbot = message.text
            msg = self.bot.send_message(message.chat.id, 'Insira Gxsessao:')
            self.bot.register_next_step_handler(msg, ler_gxsessao)

        def ler_gxsessao(message):
            self.gxsessao = message.text
            try:
                cookie_valido = self.testa_cookie(c=self.cookie, g=self.gxsessao, gb=self.gxbot)
            except:
                traceback.print_exc()
            
            if cookie_valido:
                self.data['cookie']=self.cookie
                self.data['gxsessao']=self.gxsessao
                self.data['gxbot']=self.gxbot
                
                self.data_bob['cookie']=self.cookie
                self.data_bob['gxsessao']=self.gxsessao
                self.data_bob['gxbot']=self.gxbot
                
                print(self.data, self.data_bob)
                escreve_json(os.path.join(self.PATH,'assets/auth_geoex/cookie_heli.json'),self.data)
                escreve_json(os.path.join(self.PATH,'assets/auth_geoex/cookie_ccm.json'),self.data_bob)
                self.trigger_dag(dag_id='cookie-manut')
                msg = 'Informações atualizadas com sucesso!'
            else:
                msg = 'Dados inválidos.'
            self.bot.send_message(message.chat.id, msg)

        @self.bot.message_handler(commands=['cookie_hugo'])
        def envia_cookie(message):
            msg = self.bot.send_message(message.chat.id, '''
            Atualizando informações de acesso ao Geoex.
            Insira o Cookie:
            ''')
            self.bot.register_next_step_handler(msg, read_cookie)

        def read_cookie(message):
            self.cookie = message.text
            msg = self.bot.send_message(message.chat.id, 'Insira Gxbot:')
            self.bot.register_next_step_handler(msg, read_gxbot)

        def read_gxbot(message):
            self.gxbot = message.text
            msg = self.bot.send_message(message.chat.id, 'Insira Gxsessao:')
            self.bot.register_next_step_handler(msg, read_gxsessao)

        def read_gxsessao(message):
            self.gxsessao = message.text
            try:
                cookie_valido = self.testa_cookie(c=self.cookie, g=self.gxsessao, gb=self.gxbot)
            except:
                traceback.print_exc()
            
            if cookie_valido:
                self.data_hugo['cookie']=self.cookie
                self.data_hugo['gxsessao']=self.gxsessao
                self.data_hugo['gxbot']=self.gxbot
                
                escreve_json('assets/auth_geoex/cookie_hugo.json', self.data_hugo)
                self.trigger_dag(dag_id='cookie-manut')
                msg = 'Informações atualizadas com sucesso!'
            else:
                msg = 'Dados inválidos.'
            self.bot.send_message(message.chat.id, msg)

        @self.bot.message_handler(func=lambda message: True)
        def echo_message(message):
            self.bot.send_message(message.chat.id, 'Comando Inválido.')

        self.bot.infinity_polling()