from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pendulum
from datetime import datetime

import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)


from src.bigquery import BigQuery
CLIENT_BIGQUERY = BigQuery()

from src.envia_email import enviaEmail


def gera_graficos(df):

    df['UNIDADE'] = df['UNIDADE'].map({'VITÓRIA DA CONQUISTA': 'CONQUISTA', 'BOM JESUS DA LAPA': 'LAPA'}).fillna(df['UNIDADE'])

    df_gerencia = (
        df.groupby("OPERACAO")
        .agg({
            "VALOR TOTAL": "sum"
        })
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )

    df_operacao = (
        df.groupby(["OPERACAO","UNIDADE"])
        .agg({
            "VALOR TOTAL": "sum"
        })
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )

    df_supervisor = (
        df.groupby(["OPERACAO", "SUPERVISOR"])
        .agg({
            "VALOR TOTAL": "sum"
        })
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )


    def formatar_valor(valor):
        if valor >= 1_000_000:
            return f'R$ {valor/1_000_000:.1f}M'
        elif valor >= 1_000:
            return f'R$ {valor/1_000:.1f}K'
        else:
            return f'R$ {valor:,.0f}'


    fig = plt.figure(figsize=(18, 12))

    # Grid:
    # 2 linhas x 2 colunas
    # gráfico inferior ocupa a linha inteira
    gs = fig.add_gridspec(
        2, 2,
        height_ratios=[1, 1.2],
        width_ratios=[1, 2]
    )

    ax1 = fig.add_subplot(gs[0, 0])
    ax2 = fig.add_subplot(gs[0, 1])
    ax3 = fig.add_subplot(gs[1, :])  # ocupa toda a largura


    # ==================================
    # GRÁFICO 1 — Operação
    # ==================================
    graf1 = sns.barplot(
        data=df_gerencia,
        x="OPERACAO",
        y="VALOR TOTAL",
        hue="OPERACAO",
        ax=ax1
    )

    ax1.set_title("Valor Total por Operação")
    ax1.set_xlabel("Operação")
    ax1.set_ylabel("Valor Total")

    for container in graf1.containers:
        labels = [formatar_valor(v.get_height()) for v in container]

        graf1.bar_label(
            container,
            labels=labels,
            padding=3,
            fontsize=12
        )


    # ==================================
    # GRÁFICO 2 — Unidade
    # ==================================
    graf2 = sns.barplot(
        data=df_operacao,
        x="UNIDADE",
        y="VALOR TOTAL",
        hue="OPERACAO",
        ax=ax2
    )

    ax2.set_title("Valor Total por Unidade")
    ax2.set_xlabel("Unidade")
    ax2.set_ylabel("Valor Total")

    for container in graf2.containers:
        labels = [formatar_valor(v.get_height()) for v in container]

        graf2.bar_label(
            container,
            labels=labels,
            padding=3,
            fontsize=12
        )


    # ==================================
    # GRÁFICO 3 — Supervisor
    # ==================================
    graf3 = sns.barplot(
        data=df_supervisor,
        x="SUPERVISOR",
        y="VALOR TOTAL",
        hue="OPERACAO",
        ax=ax3
    )

    ax3.set_title("Valor Total por Supervisor")
    ax3.set_xlabel("Supervisor")
    ax3.set_ylabel("Valor Total")

    # Rotaciona nomes dos supervisores
    ax3.tick_params(axis='x', rotation=45)

    for container in graf3.containers:
        labels = [formatar_valor(v.get_height()) for v in container]

        graf3.bar_label(
            container,
            labels=labels,
            padding=3,
            fontsize=12
        )


    plt.tight_layout()

    plt.savefig(
        "assets/figures/pendencias_asbuilt.png",
        dpi=300,
        bbox_inches="tight"
    )

    plt.close()


def enviar_email(df):

    def formatar_moeda(valor):
        return f"R$ {valor:,.0f}".replace(",", ".")

    
    def colorir_notas(val):
        if val >= 7:
            return 'background-color: red; color: white'
        elif val >= 3:
            return 'background-color: #FFC107; color: black'
        else:
            return 'background-color: green; color: white'
        

    df["VALOR TOTAL"] = df["VALOR TOTAL"].apply(formatar_moeda)
    df = df.sort_values(by="DIAS_ATRASO", ascending=False)

    tabela = df.style.applymap(colorir_notas, subset=['DIAS_ATRASO'])\
                .hide(axis='index')\
                .to_html(
                    index=False,
                    border=0,
                    classes="tabela"
                )



    # =========================================
    # EMAIL
    # =========================================

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

    <h3>Movimentações pendentes</h3>
    <img src="cid:img_0">

        {tabela}
    </body>

    """


    enviaEmail('teste asbuilt', html, 'hugo.viana@sirtec.com.br', imagens_corpo_email=['assets/figures/pendencias_asbuilt.png'])


def leitura_base_asbuilt():
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

    df = CLIENT_BIGQUERY.query_bigquery_table(query)
    return df



if __name__ == "__main__":
    df = leitura_base_asbuilt()
    gera_graficos(df)
    enviar_email(df)



default_args = {
    'depends_on_past' : False,
    'owner' : 'hugo',
    'retries' : 3,
    'retry_delay' : pendulum.duration(minutes=5)
}


with DAG(
    'enviar_email_movimentacoes_v6',
    schedule='0 9 * * 1-5',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    max_active_runs = 1,
    tags = ['e-mail']
):
        
    gera_relatorio = PythonOperator(
        task_id="gera_relatorio",
        python_callable=main,
    )

    enviar = PythonOperator(
        task_id="enviar_email",
        python_callable=main,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )


    gera_relatorio >> enviar