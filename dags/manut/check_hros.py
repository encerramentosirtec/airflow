# import os
# import sys
# PATH = os.getenv('AIRFLOW_HOME')
# os.chdir(PATH)
# sys.path.insert(0, PATH)

import pandas as pd

from src.geoex import Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.checklist_fechamento import checklist

from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('sirtec-bot.json')

projetos = ['B-1259084'] # Substituir por consulta de projetos ativos

r = GEOEX.consulta_hro_pastas(projetos)
if r['sucess']:
    df_hros = pd.DataFrame(r['data'])
    for hro in df_hros:
        r = GEOEX.consulta_hro(hro['Serial'])
        if r['sucess']:
            items = r['data']['Item']
            df_analises = pd.DataFrame(items['Analises'])
            df_analises = df_analises.query("Quantidade > 0")[['Grupo', 'Codigo', 'Nome', 'Quantidade']]
            analise = checklist(df_analises)
            analise = analise.assign(
                        Projeto='projeto', 
                        HRO=items['Serial'], 
                        Responsavel=items['HistoricoAtual']['Envio'], 
                        Data=items['HistoricoAtual']['Data'], 
                        Historico_id = items['HistoricoAtual']['HistoricoId']
                    )
            print(analise)
            GS_SERVICE.atualiza_planilha(
                url='https://docs.google.com/spreadsheets/d/13WrdiVEuHFQTio0JhKLODyW_-yIYI6oFMQWG2n7uVnY/edit?gid=0#gid=0',
                aba='Análises',
                df=analise,
                input_option='USER_ENTERED'
            )
else:
    print(f"Erro ao consultar pastas dos projetos: {r['message']}")
        