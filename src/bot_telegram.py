import os
import json
import telebot
import logging
import traceback
from hooks.geoex_hook import GeoexHook
from airflow.api.client.local_client import Client

PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código

def abre_json(arquivo):
    with open(arquivo) as dados:
        file = json.load(dados)
    return file

def escreve_json(arquivo, dicionario):
    with open(arquivo, "w") as outfile: 
        json.dump(dicionario, outfile)

def testa_cookie(c='', g='', gb=''):
    url_geo = 'Cadastro/ConsultarProjeto/Item'
    if c=='' and g=='' and gb=='':
        return False
    id_projeto = ''
    r = ''
    projeto = 'B-1131975'

    cookie = {
        'Cookie': c,
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/126.0.0.0 Safari/537.36 Edg/126.0.0.0',
        'Gxsessao': g,
        'Gxbot': gb
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

def push_cookie(ti):
    data_new = abre_json('dags/_internal/cookie_heli.json')
    cookie_new = data_new['cookie']
    gxsessao_new = data_new['gxsessao']
    useragent_new = data_new['useragent']
    gxbot_new = data_new['gxbot']
    ti.xcom_push(key='cookie_manut', value=cookie_new)
    ti.xcom_push(key='gxsessao_manut', value=gxsessao_new)
    ti.xcom_push(key='useragent_manut', value=useragent_new)
    ti.xcom_push(key='gxbot_manut', value=gxbot_new)

class Bots:

    def __init__(self):
        os.environ['NO_PROXY'] = '*'
        API_TOKEN = '6394622366:AAH0NNKN2pGbdB6u-erOmHgdg7FWoPnvGTM'
        self.bot = telebot.TeleBot(API_TOKEN, threaded=False)
        telebot.logger.setLevel(logging.DEBUG) # Outputs debug messages to console.
        c = Client(None, None)
        self.cookie, self.gxsessao, self.gxbot = '', '', ''
        self.data = abre_json('assets/auth_geoex/cookie_heli.json')
        self.data_bob = abre_json('assets/auth_geoex/cookie_ccm.json')
        self.data_hugo = abre_json('assets/auth_geoex/cookie_hugo.json')

    def run_bot(self):
        print("Iniciando bot")

        @self.bot.message_handler(commands=['help', 'start'])
        def send_welcome(message):
            self.bot.send_message(message.chat.id, """Bot para atualização das planilhas do Fechamento
            Comandos:
            /help - Ajuda sobre o funcionamento do bot 
            /cookie - Atualizar cookie e gxsessao
            /cookie_hugo - Atualizar cookie e gxsessao
            """)

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
                cookie_valido = testa_cookie(c=self.cookie, g=self.gxsessao, gb=self.gxbot)
            except:
                traceback.print_exc()
            
            if cookie_valido:
                self.data['cookie']=self.cookie
                self.data['gxsessao']=self.gxsessao
                self.data['gxbot']=self.gxbot
                
                self.data_bob['cookie']=self.cookie
                self.data_bob['gxsessao']=self.gxsessao
                self.data_bob['gxbot']=self.gxbot
                
                escreve_json('assets/auth_geoex/cookie_heli.json',self.data)
                escreve_json('assets/auth_geoex/cookie_ccm.json',self.data_bob)
                self.c.trigger_dag(dag_id='cookie-manut')
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
                cookie_valido = testa_cookie(c=self.cookie, g=self.gxsessao, gb=self.gxbot)
            except:
                traceback.print_exc()
            
            if cookie_valido:
                self.data_hugo['cookie']=self.cookie
                self.data_hugo['gxsessao']=self.gxsessao
                self.data_hugo['gxbot']=self.gxbot
                
                escreve_json('assets/auth_geoex/cookie_hugo.json', self.data_hugo)
                self.c.trigger_dag(dag_id='cookie-manut')
                msg = 'Informações atualizadas com sucesso!'
            else:
                msg = 'Dados inválidos.'
            self.bot.send_message(message.chat.id, msg)

        @self.bot.message_handler(func=lambda message: True)
        def echo_message(message):
            self.bot.send_message(message.chat.id, 'Comando Inválido.')

        self.bot.infinity_polling()