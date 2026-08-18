import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

import pandas as pd

from src.geoex import Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.checklist_fechamento import checklist

from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('sirtec-bot.json')

from src.bigquery import BigQuery
BIGQUERY = BigQuery()

def _obter_ultimo_envio(historico):
    """Retorna o registro do histórico referente ao último envio (status 'ENVIADO').<br>
    Depois de enviado, o HRO pode passar por VALIDANDO, VALIDADO e EM ANÁLISE, mas o
    identificador do envio que está sendo analisado continua sendo o do último
    'ENVIADO' registrado no histórico (não o `HistoricoAtual`, que muda a cada
    mudança de status). Retorna `None` se não houver nenhum envio no histórico.
    """
    envios = [h for h in historico if h['Nome'] == 'ENVIADO']
    if not envios:
        return None
    return max(envios, key=lambda h: h['Data'])

def _hros_verificados():
    """Retorna o conjunto de envios (HistoricoId, coluna ID_HRO na base) que já
    foram checados e registrados em `analises_hro` — usado para não repetir a
    verificação do mesmo envio de HRO. Se o HRO for reenviado (novo
    HistoricoId), ele volta a ser verificado normalmente.
    """
    query = """
        SELECT DISTINCT
            ID_HRO
        FROM `sirtec-472112.external_tables.analises_hro`
    """
    df = BIGQUERY.query_bigquery_table(query)
    return set(df['ID_HRO'])

def _listar_projetos():
    query = """
        SELECT
            PROJETO
        FROM 
            `sirtec-472112.external_tables.projetos_manut_corretiva`
        WHERE
            STATUS_PASTA IN ('PENDENTE', 'CRIADO', 'VALIDADO', 'REJEITADO')
    """
    df = BIGQUERY.query_bigquery_table(query)
    return df['PROJETO'].tolist()

projetos = _listar_projetos()
print(f"Projetos a verificar: {projetos}")

hros_verificados = _hros_verificados()

r = GEOEX.consulta_hro_pastas(projetos)
if r['sucess']:
    df_hros = r['data']
    for hro in df_hros:
        projeto = hro['ProjetoText']
        hro_id = hro['Serial']
        status_hro = hro['HistoricoStatus']['Nome']
        print(projeto, hro_id, status_hro)
        if status_hro in ('ENVIADO', 'VALIDADO', 'VALIDANDO'):
            r = GEOEX.consulta_hro(hro_id)
            if r['sucess']:
                items = r['data']['Item']
                ultimo_envio = _obter_ultimo_envio(items['Historico'])
                if ultimo_envio is None:
                    print(f"HRO {hro_id} sem envio registrado no histórico, pulando.")
                    continue
                if ultimo_envio['HistoricoId'] in hros_verificados:
                    print(f"HRO {hro_id} (envio {ultimo_envio['HistoricoId']}) já verificado, pulando.")
                    continue
                df_analises = pd.DataFrame(items['Analises'])
                df_analises = df_analises.query("Quantidade != 0")[['Grupo', 'Codigo', 'Nome', 'Quantidade']]
                analise = checklist(df_analises)
                analise = analise.assign(
                            Projeto=projeto,
                            HRO=hro['Serial'],
                            Responsavel=ultimo_envio['Usuario'],
                            Data=ultimo_envio['Data'],
                            Historico_id = ultimo_envio['HistoricoId']
                        )
                print(analise)
                GS_SERVICE.atualiza_planilha(
                    url='https://docs.google.com/spreadsheets/d/13WrdiVEuHFQTio0JhKLODyW_-yIYI6oFMQWG2n7uVnY/edit?gid=0#gid=0',
                    aba='Análises',
                    df=analise,
                    input_option='USER_ENTERED'
                )
else:
    print(f"Erro ao consultar pastas dos projetos: {r['data']}")
        