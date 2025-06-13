from airflow.providers.http.hooks.http import HttpHook
import os
import json
from time import sleep
import cloudscraper

class GeoexHook(HttpHook):

    def __init__(self, cookie):
        self.PATH = os.path.join(os.path.abspath(os.path.dirname(__file__)), '..') # Altera diretório raiz de execução do código
        self.cookie = cookie

        # with open(os.path.join(self.PATH,'assets/auth_geoex/{json_file}'), 'r') as f:
        #     self.cookie = json.load(f)
        
        #self.conn_id = 'geoex_default'
        #super().__init__(http_conn_id=self.conn_id)
        #print(os.getcwd())
        self.base_url = 'https://geoex.com.br/api/'

        self.header = {
            'Cookie': self.cookie['cookie'],
            'Gxsessao': self.cookie['gxsessao'],
            'Gxbot': self.cookie['gxbot'],
            'User-Agent': self.cookie['useragent']
            # 'Content-Type': 'application/json;charset=UTF-8'
        }
        
        self.scraper = cloudscraper.create_scraper(delay=10, browser='chrome')

    
    def create_url(self, endpoint):
        url = f"{ self.base_url }{ endpoint }"
        print(url)
        return url
    

    def connect_to_endpoint(self, url, method, **kwargs):
        #r = requests.Request(method, url, **kwargs)
        #prep = session.prepare_request(r)
        while True:
            try:
                #response = self.run_and_check(session, prep, {})
                if method=='POST':
                    response = self.scraper.post(url=url, headers = self.header, **kwargs)
                else:
                    response = self.scraper.get(url=url, headers = self.header)
                print(response.status_code)
                break
            except:
                print(response.status_code)
                if (response.status_code == 429) or (response.status_code ==500):
                    sleep(10)
                    continue
                else:
                    break

        return response
    

    def run(self, method, endpoint, **kwargs):
        #session = self.get_conn(headers=self.header)
        url_raw = self.create_url(endpoint)
        
        return self.connect_to_endpoint(url_raw, method, **kwargs)