from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import pendulum
from datetime import datetime
import matplotlib.pyplot as plt
from matplotlib.patches import Patch, Rectangle

COR_VERDE = "#16a34a"
COR_AMARELO = "#f59e0b"
COR_VERMELHO = "#dc2626"
COR_NAVY = "#1e293b"
COR_CINZA_GRADE = "#e2e8f0"
COR_CINZA_TEXTO = "#64748b"
COR_ZEBRA = "#f8fafc"

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

from src.email_dashboard import _estilo_gerencia

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

# Chaves = nome completo da unidade (saída de MAP_UNIDADE), não o código bruto.
# Precisa ser aplicado depois de normalizar UNIDADE via MAP_UNIDADE, já que as
# bases de origem (ex.: std_base_asbuilt) já trazem o nome completo em UNIDADE.
MAP_GERENCIA = {
    'IBOTIRAMA': 'EXTREMO OESTE',
    'BARREIRAS': 'EXTREMO OESTE',
    'JEQUIÉ': 'SUDOESTE',
    'CONQUISTA': 'SUDOESTE',
    'ITAPETINGA': 'SUDOESTE',
    'BRUMADO': 'OESTE',
    'LIVRAMENTO': 'OESTE',
    'GUANAMBI': 'OESTE',
    'LAPA': 'OESTE',
    'IRECÊ': 'CENTRO OESTE',
    'SERRINHA': 'CENTRO OESTE',
    'ITABERABA': 'CENTRO OESTE',
    'FEIRA DE SANTANA': 'CENTRO OESTE',
    'SEABRA': 'CENTRO OESTE',
    'SANTO AMARO': 'CENTRO OESTE',
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
        df_v6 = CLIENT_BIGQUERY.query_bigquery_table(query)
        df_v6['DIAS'] = df_v6['DIAS'].fillna(0)

        return df_v6
    except Exception as e:
        print(f"Erro ao ler a base de movimentação: {e}")
        raise(e)


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

def _painel_ranking(ax, x0, y0, largura, titulo_painel, itens, item_h=0.30):
    """
    Desenha um painel de ranking (título + lista de barras proporcionais coloridas)
    dentro de `ax`, em coordenadas absolutas (polegadas). `itens` é uma lista de
    tuplas (rótulo, valor, cor).
    """
    ax.text(x0, y0 + 0.14, titulo_painel, ha="left", va="center",
            fontsize=10.5, fontweight="bold", color=COR_NAVY)
    maximo = max((v for _, v, _ in itens), default=1) or 1
    y = y0 + 0.34
    for label, valor, cor in itens:
        ax.text(x0, y + item_h * 0.26, label, ha="left", va="center",
                fontsize=9.3, fontweight="bold", color=COR_NAVY)
        ax.text(x0 + largura, y + item_h * 0.26, _fmt_moeda(valor), ha="right", va="center",
                fontsize=9.3, color=COR_NAVY)
        barra_y = y + item_h * 0.5
        barra_altura = item_h * 0.26
        ax.add_patch(Rectangle((x0, barra_y), largura, barra_altura, facecolor="#e7e1d8", edgecolor="none"))
        largura_barra = max(valor / maximo, 0.015) * largura
        ax.add_patch(Rectangle((x0, barra_y), largura_barra, barra_altura, facecolor=cor, edgecolor="none"))
        y += item_h

def _gera_tabela_agrupada(df, caminho_saida, titulo, colunas, col_valor, top_n=None, col_dias=None):
    """
    Gera uma imagem de tabela agrupada por Gerência -> Operação (Unidade) ->
    Supervisor -> Projetos, replicando a estrutura e as cores por gerência do
    dashboard enviado por e-mail (ver src/email_dashboard.py::_tabela_detalhada).
    Cada nível de agrupamento tem sua própria linha de total, e a última linha
    traz o total geral. Por padrão traz todos os projetos; `top_n`, se
    informado, aplica um recorte pelos de maior valor.
    """
    df = df.sort_values(col_valor, ascending=False)
    if top_n:
        df = df.head(top_n)
    df = df.copy()

    resumo_gerencia = df.groupby("GERENCIA")[col_valor].sum().sort_values(ascending=False)
    n_unidades = sum(df.loc[df["GERENCIA"] == g, "UNIDADE"].nunique() for g in resumo_gerencia.index)
    n_supervisores = sum(df.loc[df["GERENCIA"] == g, "SUPERVISOR"].nunique() for g in resumo_gerencia.index)
    n_projetos = len(df)

    margem_x = 0.2
    largura_util = 8.6
    fig_w = largura_util + 2 * margem_x
    row_h, uni_h, band_h, header_h, total_h, titulo_h, footer_h = 0.36, 0.38, 0.42, 0.42, 0.42, 0.85, 0.32

    fig_h = (
        titulo_h + header_h
        + len(resumo_gerencia) * band_h
        + n_unidades * uni_h
        + n_supervisores * row_h
        + n_projetos * row_h
        + total_h + footer_h
    )

    fig = plt.figure(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    x_pos = [margem_x]
    for c in colunas:
        x_pos.append(x_pos[-1] + c["width"] * largura_util)
    idx_valor = next(i for i, c in enumerate(colunas) if c["key"] == col_valor)
    idx_dias = next((i for i, c in enumerate(colunas) if c["key"] == col_dias), None)

    def texto_col(idx, y, altura, texto, cor=COR_NAVY, peso="normal", tam=10.5):
        col = colunas[idx]
        x0, x1 = x_pos[idx], x_pos[idx + 1]
        xt = x0 + 0.12 if col["align"] == "left" else x1 - 0.12
        ax.text(xt, y + altura / 2, texto, ha=col["align"], va="center",
                fontsize=tam, color=cor, fontweight=peso)

    def fundo(y, altura, cor, x0=margem_x, largura=largura_util):
        ax.add_patch(Rectangle((x0, y), largura, altura, facecolor=cor, edgecolor="none", zorder=1))

    ax.text(margem_x, titulo_h * 0.4, titulo, ha="left", va="center",
            fontsize=16, fontweight="bold", color=COR_NAVY)
    ax.text(margem_x, titulo_h * 0.8,
            f"Valor total das pendências listadas: {_fmt_moeda(df[col_valor].sum())} · {n_projetos} projetos",
            ha="left", va="center", fontsize=10.5, color=COR_CINZA_TEXTO)

    y = titulo_h
    fundo(y, header_h, COR_NAVY)
    for i, c in enumerate(colunas):
        texto_col(i, y, header_h, c["label"], cor="white", peso="bold", tam=11)
    y += header_h

    for ger in resumo_gerencia.index:
        estilo = _estilo_gerencia(ger)
        df_ger = df[df["GERENCIA"] == ger]
        n_proj_ger = len(df_ger)

        fundo(y, band_h, estilo["header_bg"])
        fundo(y, 0.045, estilo["accent"])
        ax.text(margem_x + 0.12, y + band_h / 2, f"■  {str(ger).title()}", ha="left", va="center",
                fontsize=11.5, fontweight="bold", color=COR_NAVY)
        ax.text(margem_x + largura_util - 0.12, y + band_h / 2,
                f"Total: {_fmt_moeda(resumo_gerencia[ger])}  ·  {n_proj_ger}p",
                ha="right", va="center", fontsize=10, fontweight="bold", color=COR_NAVY)
        y += band_h

        resumo_uni = df_ger.groupby("UNIDADE")[col_valor].sum().sort_values(ascending=False)
        for uni, valor_uni in resumo_uni.items():
            df_uni = df_ger[df_ger["UNIDADE"] == uni]
            n_proj_uni = len(df_uni)

            fundo(y, uni_h, "white")
            ax.add_patch(Rectangle((margem_x, y), 0.06, uni_h, facecolor=estilo["accent"],
                                    edgecolor="none", zorder=2))
            ax.text(margem_x + 0.26, y + uni_h / 2, str(uni).title(), ha="left", va="center",
                    fontsize=10.3, fontweight="bold", color=COR_NAVY)
            ax.text(margem_x + largura_util - 0.12, y + uni_h / 2,
                    f"{_fmt_moeda(valor_uni)}  ·  {n_proj_uni}p",
                    ha="right", va="center", fontsize=9.5, fontweight="bold", color=COR_NAVY)
            y += uni_h

            agg = {"PROJETOS": ("PROJETO", "count")}
            if col_dias:
                agg["DIAS_SUP"] = (col_dias, "mean")
            resumo_sup = (
                df_uni.groupby("SUPERVISOR")
                .agg(VALOR_SUP=(col_valor, "sum"), **agg)
                .sort_values("VALOR_SUP", ascending=False)
            )

            for sup, srow in resumo_sup.iterrows():
                fundo(y, row_h, estilo["subtotal_bg"])
                ax.text(margem_x + 0.42, y + row_h / 2, str(sup) if pd.notna(sup) else "-",
                        ha="left", va="center", fontsize=10, fontweight="bold", color=COR_NAVY)
                partes = [f"{int(srow['PROJETOS'])}p"]
                if col_dias:
                    partes.append(f"média {srow['DIAS_SUP']:.0f}d")
                info_idx = idx_dias
                if info_idx is not None and info_idx != idx_valor:
                    texto_col(info_idx, y, row_h, " · ".join(partes), cor=COR_CINZA_TEXTO, tam=9.3)
                else:
                    ax.text(margem_x + largura_util - 0.12, y + row_h / 2, " · ".join(partes),
                            ha="right", va="center", fontsize=9.3, color=COR_CINZA_TEXTO)
                texto_col(idx_valor, y, row_h, _fmt_moeda(srow["VALOR_SUP"]), peso="bold")
                y += row_h

                df_proj = df_uni[df_uni["SUPERVISOR"] == sup].sort_values(col_valor, ascending=False)
                for j, (_, prow) in enumerate(df_proj.iterrows()):
                    bg = estilo["row_a"] if j % 2 == 0 else estilo["row_b"]
                    fundo(y, row_h, bg)
                    for i, c in enumerate(colunas):
                        valor_bruto = prow[c["key"]]
                        texto = c["fmt"](valor_bruto)
                        if col_dias and c["key"] == col_dias:
                            cor_badge = cor_atraso(valor_bruto)
                            x0, x1 = x_pos[i], x_pos[i + 1]
                            ax.add_patch(Rectangle((x0, y), x1 - x0, row_h, facecolor=cor_badge,
                                                    edgecolor="white", linewidth=1, zorder=2))
                            texto_col(i, y, row_h, texto,
                                      cor="black" if cor_badge == COR_AMARELO else "white", peso="bold", tam=10)
                        else:
                            texto_col(i, y, row_h, texto, peso="bold" if c["key"] == col_valor else "normal", tam=10)
                    y += row_h

    fundo(y, total_h, "#2E2A27")
    ax.text(margem_x + 0.12, y + total_h / 2, f"Total geral ({n_projetos} projetos)",
            ha="left", va="center", fontsize=11.5, fontweight="bold", color="white")
    texto_col(idx_valor, y, total_h, _fmt_moeda(df[col_valor].sum()), cor="white", peso="bold", tam=11.5)
    if idx_dias is not None:
        texto_col(idx_dias, y, total_h, f"{df[col_dias].mean():.0f}d", cor="white", peso="bold", tam=11.5)
    y += total_h

    ax.text(fig_w - margem_x, y + footer_h / 2,
            f"Gerado em {pendulum.now('America/Bahia').format('DD/MM/YYYY HH:mm')}",
            ha="right", va="center", fontsize=8, color=COR_CINZA_TEXTO)

    plt.savefig(caminho_saida, dpi=200, facecolor="white")
    plt.close(fig)

    return caminho_saida

def gera_tabela_pendencias_asbuilt(df, top_n=None):
    colunas = [
        {"key": "PROJETO", "label": "Projeto", "width": 0.14, "align": "left", "fmt": str},
        {"key": "TITULO", "label": "Título da obra", "width": 0.42, "align": "left",
         "fmt": lambda v: _trunc(v, 34) if pd.notna(v) else "-"},
        {"key": "VALOR TOTAL", "label": "Valor a receber", "width": 0.22, "align": "right", "fmt": _fmt_moeda},
        {"key": "DIAS_ATRASO", "label": "Dias pendente", "width": 0.22, "align": "right",
         "fmt": lambda v: f"{v:.0f}d"},
    ]
    titulo = "Pendências de As-Built" + (f" — Top {top_n}" if top_n else "")
    caminho = os.path.join(PASTA_FIGURAS, "pendencias_asbuilt_whatsapp_tabela.png")
    return _gera_tabela_agrupada(
        df, caminho, titulo, colunas,
        col_valor="VALOR TOTAL", col_dias="DIAS_ATRASO", top_n=top_n,
    )

def _stat_card(ax, x0, y0, largura, altura, rotulo, valor, cor=COR_NAVY):
    ax.add_patch(Rectangle((x0, y0), largura, altura, facecolor="white",
                            edgecolor=COR_CINZA_GRADE, linewidth=1.2, zorder=1))
    ax.add_patch(Rectangle((x0, y0), largura, 0.05, facecolor=cor, edgecolor="none", zorder=2))
    ax.text(x0 + 0.16, y0 + altura * 0.42, rotulo, ha="left", va="center",
            fontsize=9.3, color=COR_CINZA_TEXTO)
    ax.text(x0 + 0.16, y0 + altura * 0.76, valor, ha="left", va="center",
            fontsize=16, fontweight="bold", color=cor)

def gera_dashboard_geral(df_asbuilt, df_mov):
    """
    Gera uma única imagem de mini-dashboard com os valores gerais de As-Built e
    Movimentação de Material lado a lado: cartões de KPI (valor total, projetos
    pendentes etc.) e o ranking de valor por Gerência de cada base.
    """
    margem_x = 0.2
    largura_util = 11.6
    fig_w = largura_util + 2 * margem_x
    titulo_h, card_h, gap_cards, gap_secao, footer_h = 0.9, 0.95, 0.16, 0.28, 0.32

    resumo_ger_asbuilt = df_asbuilt.groupby("GERENCIA")["VALOR TOTAL"].sum().sort_values(ascending=False)
    resumo_ger_mov = df_mov.groupby("GERENCIA")["VALOR"].sum().sort_values(ascending=False)
    itens_asbuilt = [(str(g).title(), v, _estilo_gerencia(g)["accent"]) for g, v in resumo_ger_asbuilt.items()]
    itens_mov = [(str(g).title(), v, _estilo_gerencia(g)["accent"]) for g, v in resumo_ger_mov.items()]

    painel_item_h = 0.30
    painel_h = 0.34 + max(len(itens_asbuilt), len(itens_mov)) * painel_item_h + 0.18

    fig_h = titulo_h + 2 * card_h + gap_cards + gap_secao + painel_h + footer_h

    fig = plt.figure(figsize=(fig_w, fig_h))
    fig.patch.set_facecolor("white")
    ax = fig.add_axes([0, 0, 1, 1])
    ax.set_xlim(0, fig_w)
    ax.set_ylim(0, fig_h)
    ax.invert_yaxis()
    ax.axis("off")

    ax.text(margem_x, titulo_h * 0.4, "Painel Geral de Pendências", ha="left", va="center",
            fontsize=17, fontweight="bold", color=COR_NAVY)
    ax.text(margem_x, titulo_h * 0.8, "As-Built e Movimentação de Material", ha="left", va="center",
            fontsize=11, color=COR_CINZA_TEXTO)

    largura_card = (largura_util - 2 * 0.16) / 3
    y = titulo_h
    cards_asbuilt = [
        ("As-Built · valor total em aberto", _fmt_moeda(df_asbuilt["VALOR TOTAL"].sum())),
        ("As-Built · projetos pendentes", f"{len(df_asbuilt)}"),
        ("As-Built · média de dias de atraso", f"{df_asbuilt['DIAS_ATRASO'].mean():.0f}d"),
    ]
    for i, (rotulo, valor) in enumerate(cards_asbuilt):
        _stat_card(ax, margem_x + i * (largura_card + 0.16), y, largura_card, card_h, rotulo, valor)
    y += card_h + gap_cards

    cards_mov = [
        ("Movimentação · valor total dos projetos", _fmt_moeda(df_mov["VALOR"].sum())),
        ("Movimentação · projetos com pendência", f"{df_mov['PROJETO'].nunique()}"),
        ("Movimentação · média de dias parado", f"{df_mov['DIAS'].mean():.0f}d"),
    ]
    for i, (rotulo, valor) in enumerate(cards_mov):
        _stat_card(ax, margem_x + i * (largura_card + 0.16), y, largura_card, card_h, rotulo, valor)
    y += card_h + gap_secao

    largura_painel = (largura_util - 0.3) / 2
    ax.add_patch(Rectangle((margem_x, y), largura_painel, painel_h,
                            facecolor="white", edgecolor=COR_CINZA_GRADE, linewidth=1.2))
    ax.add_patch(Rectangle((margem_x + largura_painel + 0.3, y), largura_painel, painel_h,
                            facecolor="white", edgecolor=COR_CINZA_GRADE, linewidth=1.2))
    _painel_ranking(ax, margem_x + 0.16, y + 0.08, largura_painel - 0.32,
                     "As-Built — Valor por Gerência", itens_asbuilt, painel_item_h)
    _painel_ranking(ax, margem_x + largura_painel + 0.3 + 0.16, y + 0.08, largura_painel - 0.32,
                     "Movimentação — Valor por Gerência", itens_mov, painel_item_h)
    y += painel_h

    ax.text(fig_w - margem_x, y + footer_h / 2,
            f"Gerado em {pendulum.now('America/Bahia').format('DD/MM/YYYY HH:mm')}",
            ha="right", va="center", fontsize=8, color=COR_CINZA_TEXTO)

    caminho = os.path.join(PASTA_FIGURAS, "pendencias_dashboard_geral.png")
    plt.savefig(caminho, dpi=200, facecolor="white")
    plt.close(fig)

    return caminho

def gera_tabela_pendencias_movimentacao(df, top_n=None):
    colunas = [
        {"key": "PROJETO", "label": "Projeto", "width": 0.14, "align": "left", "fmt": str},
        {"key": "TITULO", "label": "Título da obra", "width": 0.42, "align": "left",
         "fmt": lambda v: _trunc(v, 34) if pd.notna(v) else "-"},
        {"key": "VALOR", "label": "Valor do projeto", "width": 0.22, "align": "right", "fmt": _fmt_moeda},
        {"key": "DIAS", "label": "Dias pendente", "width": 0.22, "align": "right",
         "fmt": lambda v: f"{v:.0f}d"},
    ]
    titulo = "Pendências de Movimentação de Material" + (f" — Top {top_n}" if top_n else "")
    caminho = os.path.join(PASTA_FIGURAS, "pendencias_movimentacao_whatsapp_tabela.png")
    return _gera_tabela_agrupada(
        df, caminho, titulo, colunas,
        col_valor="VALOR", col_dias="DIAS", top_n=top_n,
    )


def envia_imagem_pendencias_asbuilt():
    df = leitura_base_asbuilt()
    df['UNIDADE'] = df['UNIDADE'].map(MAP_UNIDADE).fillna(df['UNIDADE'])
    df['GERENCIA'] = df['UNIDADE'].map(MAP_GERENCIA).fillna('')

    caminho_tabela = gera_tabela_pendencias_asbuilt(df)

    EVO_API.send_image_from_file(NUMERO_TESTE, caminho_tabela, caption="Detalhamento das pendências de asbuilt")


def envia_imagem_pendencias_movimentacao():
    df = leitura_base_movimentacao()
    df['UNIDADE'] = df['UNIDADE'].map(MAP_UNIDADE).fillna(df['UNIDADE'])
    df['GERENCIA'] = df['UNIDADE'].map(MAP_GERENCIA).fillna('')

    caminho_tabela = gera_tabela_pendencias_movimentacao(df)

    legenda = "Detalhamento das pendências de movimentação de material"

    EVO_API.send_image_from_file(NUMERO_TESTE, caminho_tabela, caption=legenda)


def envia_imagem_pendencias_geral():
    df_asbuilt = leitura_base_asbuilt()
    df_asbuilt['UNIDADE'] = df_asbuilt['UNIDADE'].map(MAP_UNIDADE).fillna(df_asbuilt['UNIDADE'])
    df_asbuilt['GERENCIA'] = df_asbuilt['UNIDADE'].map(MAP_GERENCIA).fillna('')

    df_mov = leitura_base_movimentacao()
    df_mov['UNIDADE'] = df_mov['UNIDADE'].map(MAP_UNIDADE).fillna(df_mov['UNIDADE'])
    df_mov['GERENCIA'] = df_mov['UNIDADE'].map(MAP_GERENCIA).fillna('')

    caminho = gera_dashboard_geral(df_asbuilt, df_mov)
    EVO_API.send_image_from_file(
        NUMERO_TESTE, caminho,
        caption="📊 Painel Geral de Pendências — As-Built e Movimentação de Material",
    )


if __name__ == "__main__":
    envia_imagem_pendencias_geral()
    envia_imagem_pendencias_asbuilt()
    envia_imagem_pendencias_movimentacao()


default_args = {
    'depends_on_past' : False,
    'owner' : 'hugo',
    'retries' : 3,
    'retry_delay' : pendulum.duration(minutes=5)
}


with DAG(
    'enviar_whatsapp_pendencias',
    schedule='0 8,15 * * 1-5',
    start_date=pendulum.today('America/Bahia'),
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
    tags=['whatsapp'],
):

    tarefa_pendencia_geral = PythonOperator(
        task_id="envia_imagem_pendencias_geral",
        python_callable=envia_imagem_pendencias_geral,
        trigger_rule="all_success",
    )

    tarefa_pendencia_asbuilt = PythonOperator(
        task_id="envia_imagem_pendencias_asbuilt",
        python_callable=envia_imagem_pendencias_asbuilt,
        trigger_rule="all_success",
    )

    tarefa_pendencia_movimentacao = PythonOperator(
        task_id="envia_imagem_pendencias_movimentacao",
        python_callable=envia_imagem_pendencias_movimentacao,
        trigger_rule="all_success",
    )

    tarefa_pendencia_geral >> tarefa_pendencia_asbuilt >> tarefa_pendencia_movimentacao