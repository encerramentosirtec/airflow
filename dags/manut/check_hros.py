# import os
# import sys
# PATH = os.getenv('AIRFLOW_HOME')
# os.chdir(PATH)
# sys.path.insert(0, PATH)

import pandas as pd

from src.geoex import Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.checklist_fechamento import checklist

projetos = ['B-1259083', 'B-1259095']

r = GEOEX.consulta_hro_pastas(projetos)
if r['sucess']:
    df = pd.DataFrame(r['data'])
    hro = df.iloc[1]['Serial']
    r = GEOEX.consulta_hro(hro)
    if r['sucess']:
        historico_atual = r['data']['Item']['HistoricoAtual']
        df_analise = pd.DataFrame(r['data']['Item']['Analises'])
        # print(df_analise.head())
        # print(df_analise.info())
        # df_analise.to_csv(f'analises.csv', index=False)