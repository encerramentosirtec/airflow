from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import pendulum
from datetime import datetime
import matplotlib.pyplot as plt
import seaborn as sns

import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)


from src.bigquery import BigQuery
CLIENT_BIGQUERY = BigQuery()

from src.evolution_api import EvolutionAPI
EVO_API = EvolutionAPI()

PASTA_FIGURAS = os.path.join(PATH, "assets", "figures")
NUMERO_TESTE = "557781010127"


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
CONTATOS_COORD = DF_CONTATOS.query("FUNCAO == 'COORDENADOR'").set_index('REGIAO')['TELEFONE'].to_dict()
CONTATOS_GERENTES = DF_CONTATOS.query("FUNCAO == 'GERENTE'").set_index('REGIAO')['TELEFONE'].to_dict()


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
    'SAM': 'CENTRO OESTE',
    'OPER. SUDOESTE': 'SUDOESTE',
    'OPER. EXTREMO OESTE': 'EXTREMO OESTE',
    'OPER. OESTE': 'OESTE',
    'OPER. CENTRO OESTE': 'CENTRO OESTE'
}



def leitura_base_asbuilt():
    try:
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
    except Exception as e:
        print(f"Erro ao ler a base de asbuilt: {e}")
        raise(e)

def leitura_base_movimentacao():
    try:
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
            WHERE MOV_ALMOX = 'FALSE'
                AND CHECK_FECHAMENTO = 'Pendente'
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
    except Exception as e:
        print(f"Erro ao ler a base de movimentação: {e}")
        raise(e)


def cor_atraso(dias):
    if dias >= 7:
        return "#d62728"  # vermelho
    elif dias >= 3:
        return "#ffbf00"  # amarelo
    return "#2ca02c"  # verde


def gera_grafico_pendencias_asbuilt(df):
    """
    Gera gráfico com o valor total em aberto por unidade e a média de dias de atraso.
    """
    df_unidade = (
        df.groupby("UNIDADE")
        .agg({"VALOR TOTAL": "sum", "DIAS_ATRASO": "mean"})
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )

    fig, ax = plt.subplots(figsize=(10, 6))

    barras = sns.barplot(data=df_unidade, x="UNIDADE", y="VALOR TOTAL", hue="UNIDADE", legend=False, ax=ax)
    ax.set_title("Pendências de As-Built - Valor total em aberto por unidade")
    ax.set_xlabel("")
    ax.set_ylabel("Valor total (R$)")
    ax.tick_params(axis="x", rotation=45)

    for container in barras.containers:
        labels = [f"R$ {v.get_height():,.0f}".replace(",", ".") for v in container]
        barras.bar_label(container, labels=labels, padding=3, fontsize=9)

    ax_linha = ax.twinx()
    sns.lineplot(data=df_unidade, x="UNIDADE", y="DIAS_ATRASO", marker="o", color="black", ax=ax_linha)
    ax_linha.set_ylabel("Média de dias de atraso")

    plt.tight_layout()
    caminho = os.path.join(PASTA_FIGURAS, "pendencias_asbuilt_whatsapp_grafico.png")
    plt.savefig(caminho, dpi=200, bbox_inches="tight")
    plt.close(fig)

    return caminho


def gera_tabela_pendencias_asbuilt(df, top_n=25):
    """
    Gera imagem de tabela com o detalhamento das maiores pendências, destacando o atraso por cor.
    """
    df_tabela = df.sort_values("VALOR TOTAL", ascending=False).head(top_n)
    linhas = df_tabela[["UNIDADE", "PROJETO", "SUPERVISOR", "VALOR TOTAL", "DIAS_ATRASO"]].values

    fig, ax = plt.subplots(figsize=(12, 0.4 * len(linhas) + 1.5))
    ax.axis("off")

    dados_formatados = [
        [
            row[0],
            row[1],
            row[2],
            f"R$ {row[3]:,.0f}".replace(",", "."),
            f"{row[4]:.0f}",
        ]
        for row in linhas
    ]

    tabela = ax.table(
        cellText=dados_formatados,
        colLabels=["Unidade", "Projeto", "Supervisor", "Valor", "Dias atraso"],
        cellLoc="center",
        loc="center",
    )
    tabela.auto_set_font_size(False)
    tabela.set_fontsize(9)
    tabela.scale(1, 1.5)

    for i, row in enumerate(linhas, start=1):
        cor = cor_atraso(row[4])
        tabela[(i, 4)].set_facecolor(cor)
        tabela[(i, 4)].set_text_props(color="black" if cor == "#ffbf00" else "white")

    for j in range(5):
        tabela[(0, j)].set_facecolor("#404040")
        tabela[(0, j)].set_text_props(color="white", weight="bold")

    ax.set_title(f"Pendências de As-Built - Top {len(linhas)} projetos", fontsize=12, weight="bold", pad=20)

    plt.tight_layout()
    caminho = os.path.join(PASTA_FIGURAS, "pendencias_asbuilt_whatsapp_tabela.png")
    plt.savefig(caminho, dpi=200, bbox_inches="tight")
    plt.close(fig)

    return caminho


def envia_imagem_pendencias_asbuilt():
    df = leitura_base_asbuilt()
    df['GERENCIA'] = df['UNIDADE'].map(MAP_GERENCIA).fillna('')
    df['UNIDADE'] = df['UNIDADE'].map(MAP_UNIDADE).fillna(df['UNIDADE'])

    caminho_grafico = gera_grafico_pendencias_asbuilt(df)
    caminho_tabela = gera_tabela_pendencias_asbuilt(df)

    valor_total = df['VALOR TOTAL'].sum()
    legenda = f"⚠️ Pendências de As-Built ⚠️\n\n💵 Valor total em aberto: R$ {valor_total:,.0f}".replace(",", ".")

    EVO_API.send_image_from_file(NUMERO_TESTE, caminho_grafico, caption=legenda)
    EVO_API.send_image_from_file(NUMERO_TESTE, caminho_tabela, caption="Detalhamento das maiores pendências")


if __name__ == "__main__":
    envia_imagem_pendencias_asbuilt()