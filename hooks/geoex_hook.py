import os
import cloudscraper
from time import sleep
from airflow.providers.http.hooks.http import HttpHook

class GeoexHook(HttpHook):

    def __init__(self, cookie):
        self.PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código
        self.cookie = cookie

        self.base_url = 'https://geoex.com.br/api/'

        self.header = {
            'Cookie': self.cookie['cookie'],
            'Gxsessao': self.cookie['gxsessao'],
            'Gxbot': self.cookie['gxbot'],
            'User-Agent': self.cookie['useragent']
        }
        
        self.scraper = cloudscraper.create_scraper(delay=10, browser='chrome')

    
    def create_url(self, endpoint):
        url = f"{ self.base_url }{ endpoint }"
        #print(url)
        return url
    

    def connect_to_endpoint(self, url, method, **kwargs):
        while True:
            try:
                if method=='POST':
                    response = self.scraper.post(url=url, headers = self.header, **kwargs)
                else:
                    response = self.scraper.get(url=url, headers = self.header)
                
                resposta = response.json()
                if (response.status_code == 429) or (response.status_code == 500):
                    sleep(10)
                    continue
                elif resposta['StatusCode'] == 403:
                    raise TypeError("status code 403 - atualizar cookie")
                else:
                    break
            except Exception as e:
                print(f'Status code: {response.status_code}')
                raise e

        return response
    

    def run(self, method, endpoint, **kwargs):
        url_raw = self.create_url(endpoint)
        
        return self.connect_to_endpoint(url_raw, method, **kwargs)