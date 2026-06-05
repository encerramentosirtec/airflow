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

DF_ASBUILT = None

def gera_graficos(df):

    df['UNIDADE'] = df['UNIDADE'].map({'VITÓRIA DA CONQUISTA': 'CONQUISTA', 'BOM JESUS DA LAPA': 'LAPA'}).fillna(df['UNIDADE'])

    df_gerencia = (
        df.groupby("OPERACAO")
        .agg({
            "VALOR TOTAL": "sum",
            "DIAS_ATRASO": "mean"
        })
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )
    df_gerencia['DIAS_ATRASO'] = df_gerencia['DIAS_ATRASO'].round(0)

    df_operacao = (
        df.groupby(["OPERACAO","UNIDADE"])
        .agg({
            "VALOR TOTAL": "sum",
            "DIAS_ATRASO": "mean"
        })
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )
    df_operacao['DIAS_ATRASO'] = df_operacao['DIAS_ATRASO'].round(0)

    df_supervisor = (
        df.groupby(["OPERACAO", "SUPERVISOR"])
        .agg({
            "VALOR TOTAL": "sum",
            "DIAS_ATRASO": "mean"
        })
        .reset_index()
        .sort_values("VALOR TOTAL", ascending=False)
    )
    df_supervisor['DIAS_ATRASO'] = df_supervisor['DIAS_ATRASO'].round(0)


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
        y="VALOR TOTAL",
        hue="OPERACAO",
        ax=ax1
    )

    ax1.set_title("Valor Total por Operação")
    ax1.set_xlabel("Operação")
    ax1.set_ylabel("Valor Total")

    # Linha de média de atraso
    ax1_linha = ax1.twinx()

    sns.lineplot(
        data=df_gerencia,
        x="OPERACAO",
        y="DIAS_ATRASO",
        marker="o",
        color="red",
        linewidth=1,
        ax=ax1_linha,
        label="Média Dias Atraso",
    )

    ax1_linha.set_ylabel("Média Dias Atraso", color="black")
    ax1_linha.tick_params(axis='y', labelcolor='black')

    max_dias = df_gerencia['DIAS_ATRASO'].max()
    ax1_linha.set_ylim(0, max_dias + max_dias*0.5)

    for x, y in zip(range(len(df_gerencia)), df_gerencia["DIAS_ATRASO"]):
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
        y="VALOR TOTAL",
        hue="OPERACAO",
        ax=ax2
    )

    ax2.set_title("Valor Total por Unidade")
    ax2.set_xlabel("Unidade")
    ax2.set_ylabel("Valor Total")

    # Linha de média de atraso
    ax2_linha = ax2.twinx()

    sns.lineplot(
        data=df_operacao,
        x="UNIDADE",
        y="DIAS_ATRASO",
        marker="o",
        color="red",
        linewidth=1,
        ax=ax2_linha,
        label="Média Dias Atraso",
        legend=False
    )

    ax2_linha.set_ylabel("Média Dias Atraso", color="black")
    ax2_linha.tick_params(axis='y', labelcolor='black')

    max_dias = df_operacao['DIAS_ATRASO'].max()
    ax2_linha.set_ylim(0, max_dias + max_dias*0.5)

    for x, y in zip(range(len(df_operacao)), df_operacao["DIAS_ATRASO"]):
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
        y="VALOR TOTAL",
        hue="OPERACAO",
        ax=ax3,
    )

    ax3.set_title("Valor Total por Supervisor")
    ax3.set_xlabel("Supervisor")
    ax3.set_ylabel("Valor Total")

    ax3.tick_params(axis='x', rotation=45)

    # Linha de média de atraso
    ax3_linha = ax3.twinx()

    sns.lineplot(
        data=df_supervisor,
        x="SUPERVISOR",
        y="DIAS_ATRASO",
        marker="o",
        color="red",
        linewidth=1,
        ax=ax3_linha,
        label="Média Dias Atraso",
        legend=False
        )

    ax3_linha.set_ylabel("Média Dias Atraso", color="black")
    ax3_linha.tick_params(axis='y', labelcolor='black')

    max_dias = df_supervisor['DIAS_ATRASO'].max()
    ax3_linha.set_ylim(0, max_dias + max_dias*0.5)

    for x, y in zip(range(len(df_supervisor)), df_supervisor["DIAS_ATRASO"]):
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
        "assets/figures/pendencias_asbuilt.png",
        dpi=200,
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
    df = df.sort_values(by="DIAS_ATRASO", ascending=False).rename(columns={
        "DIAS_ATRASO": "DIAS EM ATRASO"
        })

    tabela = df.style.applymap(colorir_notas, subset=['DIAS EM ATRASO'])\
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

    # destinatarios = ["hugo.viana@sirtec.com.br"]

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

            img {{
                width: 100%;
                max-width: 100%;
                height: auto;
                display: block;
                margin-top: 15px;
                margin-bottom: 20px;
            }}

        </style>

    </head>

    <body>
    <h2>Indicadores Diários</h2>

    <h3>Pendências de As-Built</h3>
    <img src="cid:img_0">

        {tabela}
    </body>

    """


    enviaEmail('Relatório encerramento - Pendências As-Built', html, destinatarios, imagens_corpo_email=['assets/figures/pendencias_asbuilt.png'])



def leitura_base_asbuilt():
    global DF_ASBUILT
    
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

    DF_ASBUILT = CLIENT_BIGQUERY.query_bigquery_table(query)
    



if __name__ == "__main__":
    leitura_base_asbuilt()
    gera_graficos(DF_ASBUILT)
    enviar_email(DF_ASBUILT)



default_args = {
    'depends_on_past' : False,
    'owner' : 'hugo',
    'retries' : 3,
    'retry_delay' : pendulum.duration(minutes=5)
}


with DAG(
    'enviar_email_pendencia_asbuilt',
    schedule='0 9 * * 1-5',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    default_args = default_args,
    max_active_runs = 1,
    tags = ['e-mail']
):
    

    leitura_base_asbuilt = PythonOperator(
        task_id="leitura_base_asbuilt",
        python_callable=leitura_base_asbuilt,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )

    gera_graficos = PythonOperator(
        task_id="gera_graficos",
        python_callable=gera_graficos,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )

    enviar_email = PythonOperator(
        task_id="enviar_email",
        python_callable=enviar_email,
        trigger_rule="all_success",  # só roda se TODAS upstream tiverem sucesso
    )

    leitura_base_asbuilt >> gera_graficos >> enviar_email