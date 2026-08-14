# import os
# import sys
# PATH = os.getenv('AIRFLOW_HOME')
# os.chdir(PATH)
# sys.path.insert(0, PATH)

import pandas as pd

from src.geoex import Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.checklist_fechamento import checklist

projetos = ['B-1259083', 'B-1259095'] # Substituir por consulta de projetos ativos

r = GEOEX.consulta_hro_pastas(projetos)
if r['sucess']:
    df = pd.DataFrame(r['data'])
    hro = df.iloc[1]['Serial'] # Substituir por loop para percorrer todos os HROs
    r = GEOEX.consulta_hro(hro)
    if r['sucess']:
        historico_atual = r['data']['Item']['HistoricoAtual']
        df_analises = pd.DataFrame(r['data']['Item']['Analises'])
        df_analises = df_analises.query("Quantidade > 0")[['Grupo', 'Codigo', 'Nome', 'Quantidade']]
        checklist(df_analises)

        