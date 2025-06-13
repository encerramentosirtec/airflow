import os
import pandas as pd
from time import sleep

from pipeline_manut_main.hook.geoex_hook import GeoexHook

class Geoex:

    def __init__(self):
        #self.PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código
        self.PATH = os.path.join(os.getcwd(),'dags')
        #print(self.PATH)

    def confere_arquivos(self, projeto_id):
        id_pastas = {
            '411cb08a-fee6-452b-be48-0140a3da48bf': '015',
            '7764cedb-c13c-45b7-9954-788f5206c31f': '018',
            'f507578c-95a4-4057-b514-8f7325bb85af': '021',
            'e1d16ee4-e975-4006-95a0-6df3f1b6769c': '127',
            '39badd60-e8dc-4570-b045-e8aa3accf1c6': '022',
        }

        usuarios_sirtec = [
            'UENNEDE MARINHO - E966235',
            'JANAINA MOTA - E926628',
            'JANAINA MOTA - STC658503',
            'LUAN SILVA - STC853627',
            'MAXSUEL DANIEL - E961694',
            'GABRIEL PURIFICACAO - E894261',
            'STEFANI COSTA - E958545',
            'AURA CLARA - E852486',
            'HELI NERES - STC891513',
            'HELI SILVA - E926624',
            'FABRICIO OLIVEIRA - E965585',
            'JOAO CRUZ - E965583',
            'HUGO VIANA - E867407',
            'LARISSA COSTA - E871448',
            'ALEXANDRE CAIRES - E965587',
            'MATEUS COUTINHO - E972519',
            'JOSE AUGUSTO - E965586',
            'JESSICA ANJOS - STC589556',
            'ANDRESSA ROCHA - E877805',
        ]

        endpoint = 'ConsultarProjeto/Arquivo'

        json={'ProjetoId': projeto_id}

        
        while True:
            r = GeoexHook().run(endpoint, json=json)
            
            if r.status_code == 200:
                arquivos_015 = 0
                arquivos_018 = 0
                arquivos_021 = 0
                arquivos_127 = 0
                arquivos_022 = 0

                for arquivo in r.json()['Content']['Historico']:
                    if arquivo['Usuario'] in usuarios_sirtec:
                        arquivo_id = id_pastas.get(arquivo['ArquivoTipoId'])
                        if arquivo_id == '015':
                            arquivos_015 += 1
                        elif arquivo_id == '018':
                            arquivos_018 += 1
                        elif arquivo_id == '021':
                            arquivos_021 += 1
                        elif arquivo_id == '127':
                            arquivos_127 += 1
                        elif arquivo_id == '022':
                            arquivos_022 += 1
                    else:
                        print(f'{arquivo['Usuario']}: {arquivo['ArquivoTipoId']}')


                return (arquivos_015, arquivos_018, arquivos_021, arquivos_127, arquivos_022)
            
            elif r.status_code == 429:
                print(r.status_code)
                sleep(30)
                


    def baixar_relatorio(self, id_relatorio):
        endpoint = 'Relatorio/Agendar'
        json = {
            "Relatorio": id_relatorio
        }

        r = GeoexHook().run('POST', endpoint, json=json)

        if r.status_code == 200:
            id = r.json()['Content']

            estagio = 1
            status_code = r.json()['StatusCode']
            while estagio == 1:

                endpoint = 'Relatorio/Status'

                json = {
                    "Id": id,
                    "Estagio": estagio,
                    "StatusCode": status_code,
                    "Tipo": "Baixar",
                }

                r = GeoexHook().run('POST', endpoint, json=json)
                
                print('estagio 1', r)

                if r.status_code == 200:
                    content = r.json()
                    if content['StatusCode'] == 100:
                        estagio = content['Content']['Estagio']
                        status_code = content['StatusCode']
                    elif content['StatusCode'] == 200:
                        estagio = content['Content']['Estagio']
                        status_code = content['StatusCode']
                    else:
                        return {'sucess': False, 'status_code': content['StatusCode'], 'data': content['Message']}
                elif r.status_code != 429:
                    return {'sucess': False, 'status_code': r.status_code, 'data': None}
                
                if estagio == 1:
                    sleep(15)
                

            while estagio == 2:
                endpoint = 'Relatorio/Status'

                json = {
                    "Id": id,
                    "Estagio": estagio,
                    "StatusCode": 100,
                    "Tipo": "Baixar",
                }

                r = GeoexHook().run('POST', endpoint, json=json)

                print('estagio 2', r.json())

                if r.status_code == 200:
                    estagio = r.json()['Content']['Estagio']

                if estagio == 2:
                    sleep(15)
            
            if estagio == 3:
                arquivo = r.json()['Content']['Arquivo']
                nome = r.json()['Content']['Nome']
        else:
            return {'sucess': False, 'status_code': r.status_code, 'data': r}

        csv = GeoexHook().run('GET', arquivo)
        #with open(os.path.join(self.PATH, f'downloads/{nome}'), 'wb') as f:
        with open(os.path.join(self.PATH,f'pipeline_manut_main/downloads/{nome}'), 'wb') as f:
            f.write(csv.content)

        return {'sucess': True}
    

    def busca_info_projeto(self, projeto):
        endpoint = 'Programacao/ConsultarProjeto/Item'
        json = {
            'id': projeto
        }

        r = GeoexHook().run('POST', endpoint, json=json)

        if r.status_code == 200:
            content = r.json()
            if content['StatusCode'] == 200:
                return {'sucess': True, 'status_code': content['StatusCode'], 'data': content['Content']}
            else:
                return {'sucess': False, 'status_code': content['StatusCode'], 'data': content['Message']}
        else:
            return {'sucess': False, 'status_code': r.status_code, 'data': None}
        

    def busca_info_envio_de_pasta(self, projeto_id):
        endpoint = 'ConsultarProjeto/EnvioPasta/Itens'

        json = {
            'ProjetoId': projeto_id,
            'Paginacao': {
                'Pagina': "1",
                'TotalPorPagina': "100"
            }
        }

        r = GeoexHook().run('POST', endpoint, json=json)
    
        if r.status_code == 200:
            content = r.json()
            if content['StatusCode'] == 200:
                return {'sucess': True, 'status_code': content['StatusCode'], 'data': content['Content']}
            else:
                return {'sucess': False, 'status_code': content['StatusCode'], 'data': content['Message']}
        else:
            return {'sucess': False, 'status_code': r.status_code, 'data': None}
        

    def busca_info_hro(self, serial):

            endpoint = 'HubRegistroOperacional/Item'
            json = {
                "Serial": serial,
            }

            r = GeoexHook().run('POST', endpoint, json=json)
            if r.status_code == 200:
                content = r.json()
                if content['StatusCode'] == 200:
                    return {'sucess': True, 'status_code': content['StatusCode'], 'data': content['Content']}
                else:
                    return {'sucess': False, 'status_code': content['StatusCode'], 'data': content['Message']}
            else:
                return {'sucess': False, 'status_code': r.status_code, 'data': ''}


    def enviar_pasta(self, projeto):

        info_projeto = self.busca_info_projeto(projeto)

        if info_projeto['sucess']:
            id_projeto = info_projeto['data']['ProjetoId']

            endpoint = 'ConsultarProjeto/EnvioPasta/Adicionar'

            json = {
                "ProjetoId": id_projeto,
                "EmpresaId": 70,
                "Confirmar": True,
                "Itens":[
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 7,
                        "EnvioPastaItem": "CHECK DE CRITICA DA ZPS48 ITEM 25 SEM PENDÊNCIA GSE X FECHAMENTO",
                        "Selecionado": True,
                        "$$hashKey": "object:6032"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 1020,
                        "EnvioPastaItem": "ATUALIZAÇÃO DOS DADOS TÉCNICOS NO SAP (ZPS09)",
                        "Selecionado": True,
                        "$$hashKey": "object:6033"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 1032,
                        "EnvioPastaItem": "RESERVAS PENDENTES (CRÍTICA 3)",
                        "Selecionado": True,
                        "$$hashKey": "object:6034"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 39,
                        "EnvioPastaItem": "015 - RELAÇÃO DAS OC'S",
                        "Selecionado": True,
                        "$$hashKey": "object:6035"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 41,
                        "EnvioPastaItem": "021 - EVIDÊNCIA DE ENERGIZAÇÃO",
                        "Selecionado": True,
                        "$$hashKey": "object:6036"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 43,
                        "EnvioPastaItem": "127 - EVIDÊNCIAS PARA ATESTO",
                        "Selecionado": True,
                        "$$hashKey": "object:6037"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 46,
                        "EnvioPastaItem": "ERRO DE TIPOLOGIA",
                        "Selecionado": True,
                        "$$hashKey": "object:6038"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 47,
                        "EnvioPastaItem": "PENDÊNCIA SUCATA",
                        "Selecionado": True,
                        "$$hashKey": "object:6039"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 1099,
                        "EnvioPastaItem": "HUB - REGISTRO OPERACIONAL",
                        "Selecionado": True,
                        "$$hashKey": "object:6040"
                    },
                    {
                        "ProjetoEnvioPastaItemId": "00000000-0000-0000-0000-000000000000",
                        "EnvioPastaItemId": 1038,
                        "EnvioPastaItem": "CRÍTICA 43 NA ZPS48 - EVIDÊNCIA",
                        "Selecionado": True,
                        "$$hashKey": "object:6041"
                    }
                ]
            }
            
            r = GeoexHook().run('POST', endpoint, json=json)

            if r.status_code == 200:
                content = r.json()
                if content['StatusCode'] == 200:
                    return {'sucess': True, 'status_code': content['StatusCode'] == 200, 'data': content['Message']}
            else:
                return {'sucess': False, 'status_code': r.status_code, 'data': None}
            
        else:
            return {'sucess': False, 'status_code': info_projeto['status_code'], 'data': info_projeto['data']}
        


    def criar_hro_em_massa(self, files):

        endpoint = 'QARegistroOperacional/AtualizacaoEmMassa/EncerramentoOnlineOperacional/Salvar'

        r = GeoexHook().run('POST', endpoint, files=files)

        if r.status_code == 200:
            content = r.json()
            if content['StatusCode'] == 100:
                print('Solicitação de criação de HROs enviada!')
                return {'sucess': True}
            else:
                return {'sucess': False}
        else:
            return {'sucess': False}


    def aceitar_hro(self, serial):
        r = self.busca_info_hro(serial)

        if r['sucess']:
            id_hro = r['data']['Item']['HubRegistroOperacionalId']
            projeto_id = r['data']['Item']['ProjetoId']
        else:
            return r['status_code']

        analises = r['data']['Item']['Analises']
        
        for analise in analises:
            if analise['Analisado'] == True:
                if analise['Quantidade'] != analise['QuantidadeAjuste']:
                    return {'sucess': False, 'status_code': 0, 'data': 'Quantidade informada diferente da quantidade validada'}
        
        endpoint = 'HubRegistroOperacional/Atesto/Aceitar'
        
        json = {
            "HubRegistroOperacionalId": id_hro,
            "ProjetoId": projeto_id,
            "Serial": serial
        }
        
        r = GeoexHook().run('POST', endpoint, json=json)

        if r.status_code == 200:
            content = r.json()
            if content['StatusCode'] == 200:
                return {'sucess': True, 'status_code': content['StatusCode'], 'data': content['Content']}
            else:
                return {'sucess': False, 'status_code': content['StatusCode'], 'data': content['Message']}
        else:
            return {'sucess': False, 'status_code': r.status_code, 'data': ''}
