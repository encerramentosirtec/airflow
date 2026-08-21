import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

from datetime import datetime
import pendulum

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

import pandas as pd

from src.geoex import Geoex
GEOEX = Geoex(cookie_file='cookie_hugo.json')

from src.checklist_fechamento import checklist

from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('sirtec-bot.json')

from src.bigquery import BigQuery
BIGQUERY = BigQuery()

from src.evolution_api import EvolutionAPI
EVO_API = EvolutionAPI()

NUMERO_WHATSAPP_ALERTA = '557781010127'  # Hugo
URL_PLANILHA_ANALISES = 'https://docs.google.com/spreadsheets/d/13WrdiVEuHFQTio0JhKLODyW_-yIYI6oFMQWG2n7uVnY/edit?gid=0#gid=0'


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


def _montar_mensagem_whatsapp(pendencias_por_hro):
    """Monta a mensagem de alerta do WhatsApp a partir das pendências (linhas com
    Status == 'VERIFICAR') encontradas em cada HRO analisado nesta execução.
    """
    linhas = [f"⚠️ Checklist de fechamento encontrou {len(pendencias_por_hro)} HRO(s) com item(ns) a verificar:"]
    for projeto, hro, responsavel, df_pendencias in pendencias_por_hro:
        linhas.append(f"\n📌 *{projeto} - {hro}*\nResponsável: {responsavel}")
        for item in df_pendencias.itertuples():
            diferenca = item.Diferenca if item.Diferenca != '' else 'N/A'
            linhas.append(f"   • {item.Item} ({item.Verificacao}) — diferença: {diferenca}")
    return "\n".join(linhas)


def checar_hros():
    """Verifica os HROs enviados/validando/validado dos projetos ativos, monta o
    checklist de fechamento de cada um, registra na planilha de Análises e envia
    um alerta por WhatsApp quando algum item do checklist ficar como 'VERIFICAR'.
    """
    projetos = _listar_projetos()
    print(f"Projetos a verificar: {projetos}")

    hros_verificados = _hros_verificados()
    pendencias_por_hro = []

    r = GEOEX.consulta_hro_pastas(projetos)
    if not r['sucess']:
        raise Exception(f"Erro ao consultar pastas dos projetos: {r['data']}")

    df_hros = r['data']
    for hro in df_hros:
        projeto = hro['ProjetoText']
        hro_id = hro['Serial']
        status_hro = hro['HistoricoStatus']['Nome']
        print(projeto, hro_id, status_hro)
        if status_hro not in ('ENVIADO', 'VALIDADO', 'VALIDANDO'):
            continue

        r_hro = GEOEX.consulta_hro(hro_id)
        if not r_hro['sucess']:
            print(f"Erro ao consultar HRO {hro_id}: {r_hro['data']}")
            continue

        items = r_hro['data']['Item']
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
                    Historico_id=ultimo_envio['HistoricoId']
                )
        analise['Data'] = pd.to_datetime(analise['Data']).dt.strftime('%d/%m/%Y %H:%M:%S')

        GS_SERVICE.atualiza_planilha(
            url=URL_PLANILHA_ANALISES,
            aba='Análises',
            df=analise,
            input_option='USER_ENTERED'
        )

        pendencias = analise[analise['Status'] == 'VERIFICAR']
        if not pendencias.empty:
            pendencias_por_hro.append((projeto, hro['Serial'], ultimo_envio['Usuario'], pendencias))

    if pendencias_por_hro:
        mensagem = _montar_mensagem_whatsapp(pendencias_por_hro)
        EVO_API.send_text_message(NUMERO_WHATSAPP_ALERTA, mensagem)

    return {
        'status': 'Ok',
        'hros_analisados': len(df_hros),
        'hros_com_pendencia': len(pendencias_por_hro),
    }


if __name__ == '__main__':
    checar_hros()


default_args = {
    'depends_on_past': False,
    'email': ['hugo.viana@sirtec.com.br'],
    'email_on_failure': True,
    'email_on_retry': False,
    'owner': 'hugo',
    'retries': 1,
    'retry_delay': pendulum.duration(seconds=60)
}


with DAG(
    dag_id='check_hros',
    tags=['manut', 'geoex'],
    schedule='0 * * * *',  # a cada hora
    default_args=default_args,
    start_date=datetime(2026, 8, 21, tzinfo=pendulum.timezone("America/Sao_Paulo")),
    catchup=False,
):

    checar_hros_task = PythonOperator(
        task_id='checar_hros',
        python_callable=checar_hros,
    )
