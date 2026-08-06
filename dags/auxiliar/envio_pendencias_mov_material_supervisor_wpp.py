from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import pendulum
import re
import matplotlib.pyplot as plt
from matplotlib.patches import Rectangle

COR_VERDE = "#16a34a"
COR_AMARELO = "#f59e0b"
COR_VERMELHO = "#dc2626"
COR_NAVY = "#1e293b"
COR_CINZA_GRADE = "#e2e8f0"
COR_CINZA_TEXTO = "#64748b"
COR_ZEBRA = "#f8fafc"
COR_DEVOLUCAO = "#2563eb"

ORDEM_TIPO_MOV = {"Saída": 0, "Devolução": 1, "Outro": 2}

plt.rcParams["font.family"] = "DejaVu Sans"

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


def leitura_base_contatos():
    try:
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
                SETOR = 'Operação' AND
                TELEFONE IS NOT NULL
            """
        df_contatos = CLIENT_BIGQUERY.query_bigquery_table(query)

        return df_contatos
    except Exception as e:
        print(f"Erro ao ler a base de contatos: {e}")
        raise (e)


def _classifica_tipo_mov(tipo_mov):
    """Classifica o TIPO_MOV em Saída ou Devolução. Valores sinalizados como
    estorno (contêm "ESTORNO" no texto) contam como devolução, assim como os
    códigos terminados em 2; os terminados em 1 são saída."""
    tipo_str = str(tipo_mov).strip().upper()
    if "ESTORNO" in tipo_str:
        return "Devolução"
    if tipo_str.endswith("2"):
        return "Devolução"
    if tipo_str.endswith("1"):
        return "Saída"
    return "Outro"


def leitura_base_materiais_pendentes():
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
            )

            SELECT
                UNIDADE,
                SETOR,
                PROJETO,
                SUPERVISOR,
                MATERIAL,
                DESCRICAO,
                QUATIDADE AS QUANTIDADE,
                TIPO_MOV
            FROM JUNCAO
            WHERE MOV_ALMOX = 'FALSE'
            """
        df_materiais = CLIENT_BIGQUERY.query_bigquery_table(query)
        df_materiais['QUANTIDADE'] = pd.to_numeric(
            df_materiais['QUANTIDADE'].astype(str).str.replace(',', '.', regex=False), errors='coerce'
        ).fillna(0)
        df_materiais['TIPO'] = df_materiais['TIPO_MOV'].apply(_classifica_tipo_mov)

        return df_materiais
    except Exception as e:
        print(f"Erro ao ler a base de materiais pendentes: {e}")
        raise (e)


def leitura_base_obras_pendentes():
    try:
        query = """
            SELECT
                UNIDADE,
                PROJETO,
                TITULO,
                SUPERVISOR,
                VALOR,
                DIAS
            FROM `sirtec-472112.external_tables.obras_pendentes_mov`
            WHERE PROJETO IS NOT NULL
            """
        df_obras = CLIENT_BIGQUERY.query_bigquery_table(query)
        df_obras['DIAS'] = df_obras['DIAS'].fillna(0)

        return df_obras
    except Exception as e:
        print(f"Erro ao ler a base de obras pendentes de movimentação: {e}")
        raise (e)


def cor_atraso(dias):
    if dias >= 7:
        return COR_VERMELHO
    elif dias >= 3:
        return COR_AMARELO
    return COR_VERDE


def _fmt_moeda(valor):
    return f"R$ {valor:,.0f}".replace(",", ".")


def _trunc(texto, max_chars):
    texto = str(texto)
    if len(texto) <= max_chars:
        return texto
    return texto[:max_chars - 1].rstrip() + "…"


def _largura_texto(fig, ax, texto, fontsize, weight="normal"):
    """Largura (em polegadas) que `texto` ocupa quando renderizado, usada
    para aproveitar melhor o espaço disponível em cada linha da tabela."""
    renderer = fig.canvas.get_renderer()
    t = ax.text(0, 0, texto, fontsize=fontsize, fontweight=weight, alpha=0)
    largura = t.get_window_extent(renderer=renderer).width / fig.dpi
    t.remove()
    return largura


def _trunc_largura(fig, ax, texto, largura_max, fontsize, weight="normal"):
    """Trunca `texto` (com reticências) pela largura real renderizada, em vez
    de um número fixo de caracteres, para não cortar antes da hora nem
    invadir a coluna de valores."""
    texto = "" if pd.isna(texto) else str(texto).strip()
    if not texto:
        return "-"
    if _largura_texto(fig, ax, texto, fontsize, weight) <= largura_max:
        return texto
    while texto and _largura_texto(fig, ax, texto + "…", fontsize, weight) > largura_max:
        texto = texto[:-1]
    return (texto + "…") if texto else "…"


def _slug(texto):
    texto = re.sub(r"[^\w\s-]", "", str(texto)).strip().lower()
    return re.sub(r"[\s_-]+", "_", texto)


def _gera_tabela_supervisor(df_projetos, df_materiais, supervisor, caminho_saida):
    """
    Gera uma imagem de tabela com as pendências de movimentação de material de
    um único supervisor. Cada projeto é exibido com nome, título, valor e dias
    pendentes em uma linha de destaque, seguida pelos materiais em aberto e
    suas respectivas quantidades logo abaixo.
    """
    df_projetos = df_projetos.sort_values("VALOR", ascending=False).copy()

    margem_x = 0.2
    largura_util = 6.8
    fig_w = largura_util + 2 * margem_x
    titulo_h, header_h, proj_h, mat_h, footer_h = 0.85, 0.42, 0.44, 0.32, 0.32

    materiais_por_projeto = {
        projeto: (
            df_materiais[df_materiais["PROJETO"] == projeto]
            .assign(_ordem_tipo=lambda d: d["TIPO"].map(ORDEM_TIPO_MOV).fillna(2))
            .sort_values(["_ordem_tipo", "QUANTIDADE"], ascending=[True, False])
        )
        for projeto in df_projetos["PROJETO"]
    }
    n_linhas_material = sum(max(len(m), 1) for m in materiais_por_projeto.values())

    fig_h = (
        titulo_h + header_h
        + len(df_projetos) * proj_h
        + n_linhas_material * mat_h
        + footer_h
    )

    fig = plt.figure(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    def fundo(y, altura, cor, x0=margem_x, largura=largura_util):
        ax.add_patch(Rectangle((x0, y), largura, altura, facecolor=cor, edgecolor="none", zorder=1))

    valor_total = df_projetos["VALOR"].sum()
    ax.text(margem_x, titulo_h * 0.4, f"Pendências de Movimentação de Material — {supervisor}",
            ha="left", va="center", fontsize=14.5, fontweight="bold", color=COR_NAVY)
    ax.text(margem_x, titulo_h * 0.8,
            f"Valor total em aberto: {_fmt_moeda(valor_total)} · {len(df_projetos)} projetos",
            ha="left", va="center", fontsize=10.5, color=COR_CINZA_TEXTO)

    x_mat_desc = margem_x + 0.42
    x_valor_dias = fig_w - margem_x - 0.12

    y = titulo_h
    fundo(y, header_h, COR_NAVY)
    ax.text(margem_x + 0.12, y + header_h / 2, "Projeto / Material", ha="left", va="center",
            fontsize=11, fontweight="bold", color="white")
    ax.text(x_valor_dias, y + header_h / 2, "Valor / Qtd.", ha="right", va="center",
            fontsize=11, fontweight="bold", color="white")
    y += header_h

    for idx, prow in enumerate(df_projetos.itertuples()):
        bg = "white" if idx % 2 == 0 else COR_ZEBRA
        fundo(y, proj_h, bg)
        cor_badge = cor_atraso(prow.DIAS)
        ax.add_patch(Rectangle((margem_x, y), 0.06, proj_h, facecolor=cor_badge, edgecolor="none", zorder=2))

        x_titulo = margem_x + 0.26
        prefixo_titulo = f"{prow.PROJETO} — "
        largura_max_titulo = (x_valor_dias - 1.25) - x_titulo - _largura_texto(fig, ax, prefixo_titulo, 10.3, "bold")
        titulo_trunc = _trunc_largura(fig, ax, prow.TITULO, largura_max_titulo, 10.3, "bold")
        titulo_proj = f"{prefixo_titulo}{titulo_trunc}"
        ax.text(x_titulo, y + proj_h / 2, titulo_proj, ha="left", va="center",
                fontsize=10.3, fontweight="bold", color=COR_NAVY)
        ax.text(x_valor_dias, y + proj_h * 0.3, _fmt_moeda(prow.VALOR),
                ha="right", va="center", fontsize=10, fontweight="bold", color=COR_NAVY)
        ax.text(x_valor_dias, y + proj_h * 0.74, f"{prow.DIAS:.0f}d pendente",
                ha="right", va="center", fontsize=9, fontweight="bold", color=cor_badge)
        y += proj_h

        df_mat = materiais_por_projeto[prow.PROJETO]
        if len(df_mat):
            for _, mrow in df_mat.iterrows():
                fundo(y, mat_h, "white")
                descricao = mrow["DESCRICAO"] if pd.notna(mrow["DESCRICAO"]) else mrow["MATERIAL"]
                tipo = mrow["TIPO"]
                cor_tipo = COR_DEVOLUCAO if tipo == "Devolução" else COR_CINZA_TEXTO
                texto_desc = f"{descricao} · {tipo}"
                texto_qtd = f"{mrow['QUANTIDADE']:.0f} un."
                largura_qtd = _largura_texto(fig, ax, texto_qtd, 9.3, "bold")
                largura_bullet = _largura_texto(fig, ax, "• ", 9.3, "normal")
                largura_max_desc = (x_valor_dias - largura_qtd - 0.15) - x_mat_desc - largura_bullet
                descricao_trunc = _trunc_largura(fig, ax, texto_desc, largura_max_desc, 9.3, "normal")
                ax.text(x_mat_desc, y + mat_h / 2, f"• {descricao_trunc}", ha="left", va="center",
                        fontsize=9.3, color=cor_tipo)
                ax.text(x_valor_dias, y + mat_h / 2, texto_qtd, ha="right", va="center",
                        fontsize=9.3, fontweight="bold", color=cor_tipo)
                y += mat_h
                ax.plot([x_mat_desc, margem_x + largura_util], [y, y], color=COR_CINZA_GRADE, linewidth=0.6, zorder=3)
        else:
            fundo(y, mat_h, "white")
            ax.text(x_mat_desc, y + mat_h / 2, "• Sem materiais pendentes", ha="left", va="center",
                    fontsize=9.3, fontstyle="italic", color=COR_CINZA_TEXTO)
            y += mat_h

    ax.text(fig_w - margem_x, y + footer_h / 2,
            f"Gerado em {pendulum.now('America/Bahia').format('DD/MM/YYYY HH:mm')}",
            ha="right", va="center", fontsize=8, color=COR_CINZA_TEXTO)

    plt.savefig(caminho_saida, dpi=200, facecolor="white")
    plt.close(fig)

    return caminho_saida


def envia_imagens_pendencias_supervisores():
    df_contatos = leitura_base_contatos()
    contatos_supervisores = (
        df_contatos.query("FUNCAO == 'Supervisor'").set_index('NOME')['TELEFONE'].to_dict()
    )

    df_projetos = leitura_base_obras_pendentes()
    df_materiais = leitura_base_materiais_pendentes()

    for supervisor in df_projetos["SUPERVISOR"].dropna().unique():
        contato_supervisor = contatos_supervisores.get(supervisor)
        if not contato_supervisor:
            print(f"Supervisor sem telefone cadastrado: {supervisor}")
            continue

        df_sup_projetos = df_projetos.query("SUPERVISOR == @supervisor")
        df_sup_materiais = df_materiais.query("SUPERVISOR == @supervisor")

        caminho = os.path.join(PASTA_FIGURAS, f"pendencias_supervisor_{_slug(supervisor)}.png")
        caminho_tabela = _gera_tabela_supervisor(df_sup_projetos, df_sup_materiais, supervisor, caminho)

        EVO_API.send_image_from_file(
            '5577981010127', caminho_tabela,
            caption=f"📦 Esses são os seus projetos com pendência de movimentação de material, {supervisor}",
        )


if __name__ == "__main__":
    envia_imagens_pendencias_supervisores()


default_args = {
    'depends_on_past': False,
    'owner': 'hugo',
    'retries': 3,
    'retry_delay': pendulum.duration(minutes=5)
}


with DAG(
    'enviar_whatsapp_pendencias_supervisores',
    schedule='0 8,15 * * 1-5',
    start_date=pendulum.today('America/Bahia'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['whatsapp'],
):

    tarefa_pendencia_supervisores = PythonOperator(
        task_id="envia_imagens_pendencias_supervisores",
        python_callable=envia_imagens_pendencias_supervisores,
        trigger_rule="all_success",
    )

    tarefa_pendencia_supervisores
