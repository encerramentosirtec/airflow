from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import pendulum
from datetime import datetime

import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)


from src.bigquery import BigQuery
CLIENT_BIGQUERY = BigQuery()

from src.waha import Waha
WAHA = Waha()


query = """
    SELECT
        NOME,
        REGIAO,
        TELEFONE,
        EMPRESA,
        FUNCAO
    FROM `sirtec-472112.external_tables.contatos`
    WHERE
        EMPRESA = 'SIRTEC' AND
        SETOR = 'OPERAÇÃO' AND
        TELEFONE IS NOT NULL
    """
DF_CONTATOS = CLIENT_BIGQUERY.query_bigquery_table(query)

CONTATOS_SUPERVISORES = DF_CONTATOS.query("FUNCAO == 'SUPERVISOR'").set_index('NOME')['TELEFONE'].to_dict()
CONTATOS_COORD = DF_CONTATOS.query("FUNCAO == 'COORDENADOR'").set_index('NOME')['TELEFONE'].to_dict()
CONTATOS_GERENTES = DF_CONTATOS.query("FUNCAO == 'GERENTE'").set_index('NOME')['TELEFONE'].to_dict()


MAP_UNIDADE = {
    'IBO': 'IBOTIRAMA',
    'BAR': 'BARREIRAS',
    'LEM': 'BARREIRAS',
    'JEQ': 'JEQUIÉ',
    'JEQUIE': 'JEQUIÉ',
    'VDC': 'CONQUISTA',
    'VITÓRIA DA CONQUISTA': 'CONQUISTA',
    'VITORIA DA CONQUISTA': 'CONQUISTA',
    'ITG': 'ITAPETINGA',
    'BRU': 'BRUMADO',
    'LIV': 'LIVRAMENTO',
    'GUA': 'GUANAMBI',
    'BJL': 'LAPA',
    'BOM JESUS DA LAPA': 'LAPA',
    'SMV': 'LAPA',
    'IRE': 'IRECÊ',
    'SER': 'SERRINHA',
    'ITA': 'ITABERABA',
    'FSA': 'FEIRA DE SANTANA',
    'SBR': 'SEABRA',
    'SAM': 'SANTO AMARO'
}

MAP_GERENCIA = {
    'IBO': 'EXTREMO OESTE',
    'BAR': 'EXTREMO OESTE',
    'LEM': 'EXTREMO OESTE',
    'JEQ': 'SUDOESTE',
    'VDC': 'SUDOESTE',
    'ITG': 'SUDOESTE',
    'BRU': 'OESTE',
    'LIV': 'OESTE',
    'GUA': 'OESTE',
    'BJL': 'OESTE',
    'SMV': 'OESTE',
    'IRE': 'CENTRO OESTE',
    'SER': 'CENTRO OESTE',
    'ITA': 'CENTRO OESTE',
    'FSA': 'CENTRO OESTE',
    'SBR': 'CENTRO OESTE',
    'SAM': 'CENTRO OESTE'
}


#============================================================================================================
#
#   FUNÇÕES AUXILIARES
#
#============================================================================================================


def envia_ranking_unidades_pend_asbuilt(df):
    mensagem = f"⚠️ Ranking das *MAIORES PENDÊNCIAS DE ASBUILT* ⚠️\n\n"
    mensagem += "Ranking das unidades com maior valor em aberto:\n\n"
    mensagem += f"🥇 {df.groupby('UNIDADE')['VALOR TOTAL'].sum().nlargest(3).index[0]} - R$ {df.groupby('UNIDADE')['VALOR TOTAL'].sum().nlargest(3).values[0]:,.0f}\n".replace(",", ".")
    mensagem += f"🥈 {df.groupby('UNIDADE')['VALOR TOTAL'].sum().nlargest(3).index[1]} - R$ {df.groupby('UNIDADE')['VALOR TOTAL'].sum().nlargest(3).values[1]:,.0f}\n".replace(",", ".")
    mensagem += f"🥉 {df.groupby('UNIDADE')['VALOR TOTAL'].sum().nlargest(3).index[2]} - R$ {df.groupby('UNIDADE')['VALOR TOTAL'].sum().nlargest(3).values[2]:,.0f}\n".replace(",", ".")

    mensagem += "\n\nRanking dos supervisores com maior valor em aberto:\n\n"
    mensagem += f"🥇 {df.groupby('SUPERVISOR')['VALOR TOTAL'].sum().nlargest(3).index[0]} - R$ {df.groupby('SUPERVISOR')['VALOR TOTAL'].sum().nlargest(3).values[0]:,.0f}\n".replace(",", ".")
    mensagem += f"🥈 {df.groupby('SUPERVISOR')['VALOR TOTAL'].sum().nlargest(3).index[1]} - R$ {df.groupby('SUPERVISOR')['VALOR TOTAL'].sum().nlargest(3).values[1]:,.0f}\n".replace(",", ".")
    mensagem += f"🥉 {df.groupby('SUPERVISOR')['VALOR TOTAL'].sum().nlargest(3).index[2]} - R$ {df.groupby('SUPERVISOR')['VALOR TOTAL'].sum().nlargest(3).values[2]:,.0f}\n".replace(",", ".")


    WAHA.send_group_message("120363409216677503", mensagem, ["557781010127"])


def enviar_pendencias_asbuilt(df):
    """
    
        ENVIA PENDENCIA DETALHADA POR UNIDADE E SUPERVISOR
    
    """

    for operacao in df['UNIDADE'].unique():
        contato_coord = CONTATOS_COORD.get(operacao, '')
        contato_gerente = CONTATOS_GERENTES.get(operacao, '')

        q_df_asbuilt = df.query(f"UNIDADE == '{operacao}'").sort_values(by="VALOR TOTAL", ascending=False)
        
        mensagem = f"⚠️ Pendências de asbuilt - *{operacao}* ⚠️\n\n"
        mensagem += f"⏱️ Tempo médio de atraso: {q_df_asbuilt['DIAS_ATRASO'].mean():.0f} dias\n"
        mensagem += f"💵 Valor total em aberto: R$ {q_df_asbuilt['VALOR TOTAL'].sum():,.0f}\n\n".replace(",", ".")

        for i in q_df_asbuilt.itertuples():
            if i.DIAS_ATRASO >= 7:
                mensagem += f"Projeto: {i.PROJETO}\nSupervisor: {i.SUPERVISOR}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🔴\n\n".replace(",", ".")
            elif i.DIAS_ATRASO >= 3:
                mensagem += f"Projeto: {i.PROJETO}\nSupervisor: {i.SUPERVISOR}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🟡\n\n".replace(",", ".")
            else:
                mensagem += f"Projeto: {i.PROJETO}\nSupervisor: {i.SUPERVISOR}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🟢\n\n".replace(",", ".")
        
        WAHA.send_group_message("120363409216677503", mensagem, [contato_coord, contato_gerente])


def enviar_pendencias_asbuilt_geral(df):
    """

        ENVIA PENDENCIAS DE ASBUILT GERAL POR UNIDADE

    """
    mensagem = f"⚠️ *PENDÊNCIA DE AS-BUILT* ⚠️\n\n"

    for gerencia in df['OPERACAO'].unique():
        df_asbuilt_gerencia = df.query(f"OPERACAO == '{gerencia}'").sort_values(by="VALOR TOTAL", ascending=False)
        mensagem += f"Gerência: *{gerencia}*\n\n"

        for operacao in df_asbuilt_gerencia['UNIDADE'].unique():
            contato_coord = CONTATOS_COORD.get(operacao, '')
            contato_gerente = CONTATOS_GERENTES.get(operacao, '')

            df_asbuilt_coord = df_asbuilt_gerencia.query(f"UNIDADE == '{operacao}'").sort_values(by="VALOR TOTAL", ascending=False)
            mensagem += f"Unidade: *{operacao}*\n"

            if df_asbuilt_coord['DIAS_ATRASO'].mean() >= 7:
                mensagem += f"  ⏱️ Tempo médio: {df_asbuilt_coord['DIAS_ATRASO'].mean():.0f} dias\n  🔴 Péssimo!\n".replace(",", ".")
            elif df_asbuilt_coord['DIAS_ATRASO'].mean() >= 3:
                mensagem += f"  ⏱️ Tempo médio: {df_asbuilt_coord['DIAS_ATRASO'].mean():.0f} dias\n  🟡 Ruím\n".replace(",", ".")
            else:
                mensagem += f"  ⏱️ Tempo médio: {df_asbuilt_coord['DIAS_ATRASO'].mean():.0f} dias\n  🟢 Bom\n".replace(",", ".")
                    
            mensagem += f"  💵 Valor total: R${df_asbuilt_coord['VALOR TOTAL'].sum():,.0f}\n".replace(",", ".")
            mensagem += f"  @{contato_coord}\n\n"

        mensagem += "-----------------------------------\n\n"

    mensoes = [
        x for x in (
            list(CONTATOS_COORD.values()) + list(CONTATOS_GERENTES.values())
        )
        if x is not None
    ]
        
    r = WAHA.send_group_message("120363071699650663", mensagem, mensoes)  # GRUPO COORD
    # r = WAHA.send_group_message("120363409216677503", mensagem, "557781010127")  # GRUPO TESTE
    return r


def enviar_pendencias_v6_geral(df):
    """

        ENVIA PENDENCIAS NA V6 GERAL POR UNIDADE

    """
    mensagem = f"⚠️ *PENDÊNCIA DE MOVIMENTAÇÃO DE MATERIAL* ⚠️\n\n"

    for gerencia in df['GERENCIA'].unique():
        df_gerencia = df.query(f"GERENCIA == '{gerencia}'").sort_values(by="VALOR", ascending=False)
        mensagem += f"Gerência: *{gerencia}*\n\n"

        for operacao in df_gerencia['UNIDADE'].unique():
            contato_coord = CONTATOS_COORD.get(operacao, '')
            contato_gerente = CONTATOS_GERENTES.get(operacao, '')

            df_coord = df_gerencia.query(f"UNIDADE == '{operacao}'").sort_values(by="VALOR", ascending=False)
            mensagem += f"Unidade: *{operacao}*\n"
                    
            mensagem += f"  📋 Quantidade de projetos: {df_coord['PROJETO'].nunique()}\n"
            mensagem += f"  🔩 Quantidade de materiais: {df_coord['QTD_MATERIAL'].sum()}\n"
            mensagem += f"  💵 Valor total: R${df_coord['VALOR'].sum():,.0f}\n".replace(",", ".")
            mensagem += f"  @{contato_coord}\n\n"

        mensagem += "-----------------------------------\n\n"
        
    # print(mensagem)


    mensoes = [
        x for x in (
            list(CONTATOS_COORD.values()) + list(CONTATOS_GERENTES.values())
        )
        if x is not None
    ]

    r = WAHA.send_group_message("120363071699650663", mensagem, mensoes)  # GRUPO COORD
    # r = WAHA.send_group_message("120363409216677503", mensagem, "557781010127")  # GRUPO TESTE
    return r


def enviar_pendencias_supervisores(df_asbuilt, df_v6):
    """

        ENVIA PENDENCIAS DETALHADAS POR SUPERVISOR

    """
    for supervisor in df_asbuilt['SUPERVISOR'].unique():
        contato_supervisor = CONTATOS_SUPERVISORES.get(supervisor, '')
        # contato_supervisor = '557781010127'

        if contato_supervisor:

            q_df_asbuilt = df_asbuilt.query(f"SUPERVISOR == '{supervisor}'").sort_values(by="VALOR TOTAL", ascending=False)
            
            mensagem = f"⚠️ Projetos com pendência de *ASBUILT*: ⚠️\n"
            for i in q_df_asbuilt.itertuples():
                if i.DIAS_ATRASO >= 7:
                    mensagem += f"\n\nProjeto: {i.PROJETO}\nPendência: {i.PENDENCIAS}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🔴\n".replace(",", ".")
                elif i.DIAS_ATRASO >= 3:
                    mensagem += f"\n\nProjeto: {i.PROJETO}\nPendência: {i.PENDENCIAS}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🟡\n".replace(",", ".")
                else:
                    mensagem += f"\n\nProjeto: {i.PROJETO}\nPendência: {i.PENDENCIAS}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🟢\n".replace(",", ".")

            WAHA.send_private_message(contato_supervisor, mensagem)

    for supervisor in df_v6['SUPERVISOR'].unique():
        contato_supervisor = CONTATOS_SUPERVISORES.get(supervisor, '')
        # contato_supervisor = '557781010127'

        q_df_v6 = df_v6.query(f"SUPERVISOR == '{supervisor}'").sort_values(by="VALOR", ascending=False)

        if contato_supervisor:
            mensagem = f"⚠️ Projetos com pendência de *MOVIMENTAÇÃO DE MATERIAL*: ⚠️"
            for i in q_df_v6.itertuples():
                mensagem += f"\n\nProjeto: {i.PROJETO}\nQtd. de materiais: {i.QTD_MATERIAL} unidades\nValor do projeto: R$ {i.VALOR:,.0f}\n".replace(",", ".")
            WAHA.send_private_message(contato_supervisor, mensagem)


def enviar_pendencias_gerentes(df_asbuilt, df_v6):
    """

        ENVIA PENDENCIAS DETALHADAS POR GERENTE

    """
    for operacao in df_asbuilt['UNIDADE'].unique():
        contato_gerente = CONTATOS_GERENTES.get(operacao, '')
        if contato_gerente:

            q_df_asbuilt = df_asbuilt.query(f"UNIDADE == '{operacao}'").sort_values(by="VALOR TOTAL", ascending=False)
            q_df_v6 = df_v6.query(f"UNIDADE == '{operacao}'").sort_values(by="VALOR", ascending=False)
            
            mensagem = f"⚠️ Projetos com pendência de *ASBUILT* - {operacao}: ⚠️\n"
            for i in q_df_asbuilt.itertuples():
                if i.DIAS_ATRASO >= 7:
                    mensagem += f"\n\nProjeto: {i.PROJETO}\nPendência: {i.PENDENCIAS}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🔴\n".replace(",", ".")
                elif i.DIAS_ATRASO >= 3:
                    mensagem += f"\n\nProjeto: {i.PROJETO}\nPendência: {i.PENDENCIAS}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🟡\n".replace(",", ".")
                else:
                    mensagem += f"\n\nProjeto: {i.PROJETO}\nPendência: {i.PENDENCIAS}\nValor: R$ {i._8:,.0f}\nDias de atraso: {i.DIAS_ATRASO} 🟢\n".replace(",", ".")

            WAHA.send_private_message(contato_gerente, mensagem)

    for operacao in df_v6['UNIDADE'].unique():
        contato_gerente = CONTATOS_GERENTES.get(operacao, '')
        if contato_gerente:
            mensagem = f"⚠️ Projetos com pendência de *MOVIMENTAÇÃO DE MATERIAL* - {operacao}: ⚠️"
            for i in q_df_v6.itertuples():
                mensagem += f"\n\nProjeto: {i.PROJETO}\nQtd. de materiais: {i.QTD_MATERIAL} unidades\nValor do projeto: R$ {i.VALOR:,.0f}\n".replace(",", ".")
            WAHA.send_private_message(contato_gerente, mensagem)


def leitura_base_asbuilt():
    
    query = """
        SELECT
            OPERACAO,
            UNIDADE,
            PROJETO,
            TITULO,
            SUPERVISOR_AJUSTADO AS SUPERVISOR,
            DIAS_ATRASO,
            PENDENCIAS,
            ROUND(VALOR_PROJETO, 2) AS `VALOR TOTAL`
        FROM `sirtec-472112.standardized.std_base_asbuilt`
        """
    df_asbuilt = CLIENT_BIGQUERY.query_bigquery_table(query)

    return df_asbuilt


def leitura_base_movimentacao():

    query = """
        WITH JUNCAO AS (
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_bar`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_bjl`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_bru`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_fsa`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_gua`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_ibo`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_ire`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_ita`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_jeq`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_ser`
        UNION ALL
        SELECT * FROM `sirtec-472112.external_tables.movimentacoes_vdc`
        ),

        AGRUPADO AS (
        SELECT
            UNIDADE,
            SETOR,
            PROJETO,
            SUPERVISOR,
            COUNT(MATERIAL) AS QTD_MATERIAL
        FROM JUNCAO
        WHERE CHECK_FECHAMENTO = 'Pendente'
            AND UNIDADE IS NOT NULL
            AND PROJETO IS NOT NULL
        GROUP BY UNIDADE, SETOR, PROJETO, SUPERVISOR
        )

        SELECT
        a.*,
        b.VALOR
        FROM AGRUPADO a
        LEFT JOIN `sirtec-472112.external_tables.valor_projetos` b
        ON a.PROJETO = b.PROJETO;
        """
    
    
    df_v6 = CLIENT_BIGQUERY.query_bigquery_table(query)
    
    return df_v6




#============================================================================================================
#
#   FUNÇÕES PRINCIPAIS
#
#============================================================================================================

def envia_pendencia_asbuilt():
    df_asbuilt = leitura_base_asbuilt()
    df_asbuilt['GERENCIA'] = df_asbuilt['UNIDADE'].map(MAP_GERENCIA).fillna('')
    df_asbuilt['UNIDADE'] = df_asbuilt['UNIDADE'].map(MAP_UNIDADE).fillna(df_asbuilt['UNIDADE'])
    r = enviar_pendencias_asbuilt_geral(df_asbuilt)
    print(r)


def envia_pendencia_movimentacao():
    df_v6 = leitura_base_movimentacao() 
    df_v6['GERENCIA'] = df_v6['UNIDADE'].map(MAP_GERENCIA).fillna('')
    df_v6['UNIDADE'] = df_v6['UNIDADE'].map(MAP_UNIDADE).fillna(df_v6['UNIDADE'])
    r = enviar_pendencias_v6_geral(df_v6)
    print(r)


def envia_pendencia_supervisores():
    df_v6 = leitura_base_movimentacao() 
    df_v6['GERENCIA'] = df_v6['UNIDADE'].map(MAP_GERENCIA).fillna('')
    df_v6['UNIDADE'] = df_v6['UNIDADE'].map(MAP_UNIDADE).fillna(df_v6['UNIDADE'])

    df_asbuilt = leitura_base_asbuilt()
    df_asbuilt['GERENCIA'] = df_asbuilt['UNIDADE'].map(MAP_GERENCIA).fillna('')
    df_asbuilt['UNIDADE'] = df_asbuilt['UNIDADE'].map(MAP_UNIDADE).fillna(df_asbuilt['UNIDADE'])

    r = enviar_pendencias_supervisores(df_asbuilt, df_v6)
    print(r)


if __name__ == "__main__":
    # envia_pendencia_asbuilt()
    # envia_pendencia_movimentacao()
    envia_pendencia_supervisores()



#################################################
#
#   DAG
#
#################################################


default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 3,
    'retry_delay' : pendulum.duration(minutes=5),
    'tags': ['whatsapp']

}


with DAG(
    'enviar_asbuilt',
    schedule='0 9 * * 1-5',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    max_active_runs = 1,
):
        
    gera_relatorio = PythonOperator(
        task_id="envia_pendencia_asbuilt",
        python_callable=envia_pendencia_asbuilt,
    )

    envia_pendencia_asbuilt



with DAG(
    'enviar_movimentacoes_v6',
    schedule='0 9 * * 1-5',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    max_active_runs = 1,
):

    envienvia_pendencia_movimentacao = PythonOperator(
        task_id="envia_pendencia_movimentacao",
        python_callable=envia_pendencia_movimentacao,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )


    envia_pendencia_movimentacao