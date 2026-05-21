from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
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

from src.envia_email import enviaEmail

DF_V6 = None

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
    'BARREIRAS': 'EXTREMO OESTE',
    'IBOTIRAMA': 'EXTREMO OESTE',
    'JEQUIÉ': 'SUDOESTE',
    'CONQUISTA': 'SUDOESTE',
    'ITAPETINGA': 'SUDOESTE',
    'BRUMADO': 'OESTE',
    'LIVRAMENTO': 'OESTE',
    'GUANAMBI': 'OESTE',
    'LAPA': 'OESTE',
    'IRECÊ': 'CENTRO OESTE',
}


def gera_graficos(df):

    df_gerencia = (
        df.groupby("OPERACAO")
        .agg({
            "VALOR": "sum",
            "DIAS": "mean"
        })
        .reset_index()
        .sort_values("VALOR", ascending=False)
    )
    df_gerencia['DIAS'] = df_gerencia['DIAS'].round(0)

    df_operacao = (
        df.groupby(["OPERACAO","UNIDADE"])
        .agg({
            "VALOR": "sum",
            "DIAS": "mean"
        })
        .reset_index()
        .sort_values("VALOR", ascending=False)
    )
    df_operacao['DIAS'] = df_operacao['DIAS'].round(0)

    df_supervisor = (
        df.groupby(["OPERACAO", "SUPERVISOR"])
        .agg({
            "VALOR": "sum",
            "DIAS": "mean"
        })
        .reset_index()
        .sort_values("VALOR", ascending=False)
    )
    df_supervisor['DIAS'] = df_supervisor['DIAS'].round(0)
    

    def formatar_valor(valor):
        if valor >= 1_000_000:
            return f'R$ {valor/1_000_000:.1f}M'
        elif valor >= 1_000:
            return f'R$ {valor/1_000:.1f}K'
        else:
            return f'R$ {valor:,.0f}'


    fig = plt.figure(figsize=(18, 12))

    gs = fig.add_gridspec(
        2, 2,
        height_ratios=[1, 1.2],
        width_ratios=[1, 2]
    )

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1, :])


    # ==================================
    # GRÁFICO 1 — Operação
    # ==================================
    graf1 = sns.barplot(
        data=df_gerencia,
        x="OPERACAO",
        y="VALOR",
        hue="OPERACAO",
        ax=ax1
    )

    ax1.set_title("Valor por Operação")
    ax1.set_xlabel("Operação")
    ax1.set_ylabel("Valor")

    # Linha de média de atraso
    ax1_linha = ax1.twinx()

    sns.lineplot(
        data=df_gerencia,
        x="OPERACAO",
        y="DIAS",
        marker="o",
        color="red",
        linewidth=1,
        ax=ax1_linha,
        label="Média Dias Atraso",
    )

    ax1_linha.set_ylabel("Média Dias Atraso", color="black")
    ax1_linha.tick_params(axis='y', labelcolor='black')

    max_dias = df_gerencia['DIAS'].max()
    ax1_linha.set_ylim(0, max_dias + max_dias*0.5)

    for x, y in zip(range(len(df_gerencia)), df_gerencia["DIAS"]):
        ax1_linha.text(
            x,
            y,
            f'{y:.0f}',
            color='black',
            fontsize=12,
            ha='center',
            va='bottom',
        )

    # Labels barras
    for container in graf1.containers:
        labels = [formatar_valor(v.get_height()) for v in container]

        graf1.bar_label(
            container,
            labels=labels,
            padding=3,
            fontsize=12,
        )

    # Legendas combinadas
    handles1, labels1 = ax1.get_legend_handles_labels()
    handles2, labels2 = ax1_linha.get_legend_handles_labels()

    ax1.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc='upper right'
    )


    # ==================================
    # GRÁFICO 2 — Unidade
    # ==================================
    graf2 = sns.barplot(
        data=df_operacao,
        x="UNIDADE",
        y="VALOR",
        hue="OPERACAO",
        ax=ax2
    )

    ax2.set_title("Valor por Unidade")
    ax2.set_xlabel("Unidade")
    ax2.set_ylabel("Valor")

    # Linha de média de atraso
    ax2_linha = ax2.twinx()

    sns.lineplot(
        data=df_operacao,
        x="UNIDADE",
        y="DIAS",
        marker="o",
        color="red",
        linewidth=1,
        ax=ax2_linha,
        label="Média Dias Atraso",
        legend=False
    )

    ax2_linha.set_ylabel("Média Dias Atraso", color="black")
    ax2_linha.tick_params(axis='y', labelcolor='black')

    max_dias = df_operacao['DIAS'].max()
    ax2_linha.set_ylim(0, max_dias + max_dias*0.5)

    for x, y in zip(range(len(df_operacao)), df_operacao["DIAS"]):
        ax2_linha.text(
            x,
            y,
            f'{y:.0f}',
            color='black',
            fontsize=12,
            ha='center',
            va='bottom',
        )

    # Labels barras
    for container in graf2.containers:
        labels = [formatar_valor(v.get_height()) for v in container]

        graf2.bar_label(
            container,
            labels=labels,
            padding=3,
            fontsize=12
        )

    # # Legendas combinadas
    handles1, labels1 = ax2.get_legend_handles_labels()
    handles2, labels2 = ax2_linha.get_legend_handles_labels()

    ax2.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc='upper right'
    )

    # ==================================
    # GRÁFICO 3 — Supervisor
    # ==================================
    graf3 = sns.barplot(
        data=df_supervisor,
        x="SUPERVISOR",
        y="VALOR",
        hue="OPERACAO",
        ax=ax3,
    )

    ax3.set_title("Valor por Supervisor")
    ax3.set_xlabel("Supervisor")
    ax3.set_ylabel("Valor")

    ax3.tick_params(axis='x', rotation=45)

    # Linha de média de atraso
    ax3_linha = ax3.twinx()

    sns.lineplot(
        data=df_supervisor,
        x="SUPERVISOR",
        y="DIAS",
        marker="o",
        color="red",
        linewidth=1,
        ax=ax3_linha,
        label="Média Dias Atraso",
        legend=False
        )

    ax3_linha.set_ylabel("Média Dias Atraso", color="black")
    ax3_linha.tick_params(axis='y', labelcolor='black')

    max_dias = df_supervisor['DIAS'].max()
    ax3_linha.set_ylim(0, max_dias + max_dias*0.5)

    for x, y in zip(range(len(df_supervisor)), df_supervisor["DIAS"]):
        ax3_linha.text(
            x,
            y,
            f'{y:.0f}',
            color='black',
            fontsize=12,
            ha='center',
            va='bottom',
        )


    # Labels barras
    for container in graf3.containers:
        labels = [formatar_valor(v.get_height()) for v in container]

        graf3.bar_label(
            container,
            labels=labels,
            padding=3,
            fontsize=12
        )

    # Legendas combinadas
    handles1, labels1 = ax3.get_legend_handles_labels()
    handles2, labels2 = ax3_linha.get_legend_handles_labels()

    ax3.legend(
        handles1 + handles2,
        labels1 + labels2,
        loc='upper right'
    )


    plt.tight_layout()

    plt.savefig(
        "assets/figures/pendencias_movimentacao.png",
        dpi=300,
        bbox_inches="tight"
    )

    plt.close()


def enviar_email(df):

    def formatar_moeda(valor):
        return f"R$ {valor:,.0f}".replace(",", ".")
    
    def formatar_percentual(valor):
        return f"{valor:.0%}".replace(".", ",")

    
    def colorir_notas(val):
        if val >= 7:
            return 'background-color: red; color: white'
        elif val >= 3:
            return 'background-color: #FFC107; color: black'
        else:
            return 'background-color: green; color: white'
        

    df["VALOR"] = df["VALOR"].apply(formatar_moeda)
    df["PERC_MOV"] = df["PERC_MOV"].apply(formatar_percentual)
    df = df.sort_values(by="DIAS", ascending=False)

    df = df.rename(columns={
        "PERC_MOV": "% MOVIMENTADO",
        "STATUS_V6": "STATUS"
    })
    tabela = df.style.applymap(colorir_notas, subset=['DIAS'])\
                .hide(axis='index')\
                .to_html(
                    index=False,
                    border=0,
                    classes="tabela"
                )


    # =========================================
    # EMAIL
    # =========================================

    query = """
        SELECT
            NOME,
            REGIAO,
            EMAIL,
            EMPRESA,
            FUNCAO
        FROM `sirtec-472112.external_tables.contatos`
        WHERE
            EMPRESA = 'SIRTEC' AND
            SETOR = 'OPERAÇÃO' AND
            EMAIL IS NOT NULL
        """
    df_destinatários = CLIENT_BIGQUERY.query_bigquery_table(query)
    destinatarios = df_destinatários['EMAIL'].tolist()
    destinatarios = destinatarios + ['gabriel.brito@sirtec.com.br', 'hugo.viana@sirtec.com.br', 'gessica.pereira@sirtec.com.br', 'brenda.moreira@sirtec.com.br']

    destinatarios = [
        "hugo.viana@sirtec.com.br"
    ]


    html = f"""
    <head>

        <style>

            body {{
                font-family: Arial, Helvetica, sans-serif;
                font-size: 14px;
                color: #333333;
            }}

            .container {{
                max-width: 1100px;
            }}

            .titulo {{
                font-size: 18px;
                font-weight: bold;
                color: #2f3b52;
                margin-bottom: 10px;
            }}

            .descricao {{
                margin-bottom: 20px;
                color: #666666;
            }}

            .bloco {{
                margin-top: 30px;
            }}

            .subtitulo {{
                font-size: 15px;
                font-weight: bold;
                color: #2f3b52;
                margin-bottom: 10px;
            }}

            table {{
                border-collapse: collapse;
                width: 100%;
            }}

            th {{
                background-color: #f4f6f8;
                color: #333333;
                border: 1px solid #dddddd;
                padding: 8px;
                text-align: center;
                font-size: 13px;
            }}

            td {{
                border: 1px solid #e5e5e5;
                padding: 8px;
                text-align: center;
                font-size: 13px;
            }}

            tr:nth-child(even) {{
                background-color: #fafafa;
            }}

            .footer {{
                margin-top: 30px;
                font-size: 12px;
                color: #888888;
            }}

        </style>

    </head>

    <body>
    <h2>Indicadores Diários</h2>

    <h3>Obras pendentes de movimentação</h3>
    <img src="cid:img_0">

        {tabela}
    </body>

    """


    enviaEmail('Relatório encerramento - Obras pendentes de movimentação', html, destinatarios, imagens_corpo_email=['assets/figures/pendencias_movimentacao.png'])


def leitura_base_asbuilt():
    global DF_V6

    query = """
        SELECT
            *
        FROM 
            `sirtec-472112.external_tables.obras_pendentes_mov`
        WHERE
            PROJETO IS NOT NULL
    """

    DF_V6 = CLIENT_BIGQUERY.query_bigquery_table(query)
    DF_V6['OPERACAO'] = DF_V6['UNIDADE'].map(MAP_GERENCIA).fillna('-')
    DF_V6['DIAS'] = DF_V6['DIAS'].fillna(0)



if __name__ == "__main__":
    leitura_base_asbuilt()
    gera_graficos(DF_V6)
    enviar_email(DF_V6)



# default_args = {
#     'depends_on_past' : False,
#     'owner' : 'hugo',
#     'retries' : 3,
#     'retry_delay' : pendulum.duration(minutes=5)
# }


# with DAG(
#     'enviar_email_movimentacoes_v6',
#     schedule='0 9 * * 1-5',
#     start_date=pendulum.today('America/Sao_Paulo'),
#     catchup=False,
#     default_args = default_args,
#     max_active_runs = 1,
#     tags = ['e-mail']
# ):
        
#     gera_relatorio = PythonOperator(
#         task_id="gera_relatorio",
#         python_callable=main,
#     )

#     enviar = PythonOperator(
#         task_id="enviar_email",
#         python_callable=main,
#         trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
#     )


#     gera_relatorio >> enviar