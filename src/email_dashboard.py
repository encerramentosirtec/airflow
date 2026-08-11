from datetime import datetime
import zlib

# Cores fixas por gerência (não por posição no ranking), para que a mesma gerência
# sempre apareça com a mesma cor independente de qual teve mais valor no dia.
ESTILOS_GERENCIA = {
    'EXTREMO OESTE': {'accent': '#D5040C', 'header_bg': '#FAE1E2', 'subtotal_bg': '#FCF0F0', 'row_a': '#FFFFFF', 'row_b': '#FEF7F8'},
    'OESTE': {'accent': '#F4AB00', 'header_bg': '#FEF5E0', 'subtotal_bg': '#FEFAF0', 'row_a': '#FFFFFF', 'row_b': '#FFFCF7'},
    'SUDOESTE': {'accent': '#33475B', 'header_bg': '#E7E9EB', 'subtotal_bg': '#F3F4F5', 'row_a': '#FFFFFF', 'row_b': '#F9F9FA'},
    'CENTRO OESTE': {'accent': '#5B8A72', 'header_bg': '#E3EFE9', 'subtotal_bg': '#EBF4EF', 'row_a': '#FFFFFF', 'row_b': '#F6FAF8'},
} 

# Fallback para gerências fora do mapa acima (ex.: "-"): escolhido de forma determinística
# pelo nome (hash), não pela ordem de aparição, então continua estável entre execuções.
_ESTILOS_FALLBACK = [
    {'accent': '#7B5EA7', 'header_bg': '#EEE8F5', 'subtotal_bg': '#F5F1FA', 'row_a': '#FFFFFF', 'row_b': '#FAF8FC'},
    {'accent': '#8A6D3B', 'header_bg': '#F3ECDE', 'subtotal_bg': '#F8F3E9', 'row_a': '#FFFFFF', 'row_b': '#FCF9F3'},
]


def _estilo_gerencia(ger):
    chave = str(ger).strip().upper()
    if chave in ESTILOS_GERENCIA:
        return ESTILOS_GERENCIA[chave]
    idx = zlib.crc32(chave.encode('utf-8')) % len(_ESTILOS_FALLBACK)
    return _ESTILOS_FALLBACK[idx]


def _fmt_moeda(valor):
    return f"R$ {valor:,.0f}".replace(",", ".")


def _fmt_dias(valor):
    return f"{valor:.1f}".replace(".", ",") + "d"


def _cor_dias(valor):
    return '#D5040C' if valor >= 7 else '#262220'


def _swatch(cor, tamanho=8):
    """Marcador colorido bulletproof para Outlook: usa cor de texto (sempre suportada)
    em vez de background em <span>, que o motor Word do Outlook ignora."""
    return f'<span style="color:{cor};font-size:{tamanho}px;line-height:1;">&#9632;</span>'


def _bloco_ranking(titulo, subtitulo, itens, cores, altura_barra):
    maximo = max((valor for _, valor, _, _ in itens), default=1) or 1
    linhas = ""
    for (rotulo, valor, projetos, dias), cor in zip(itens, cores):
        largura = round(valor / maximo * 100, 1)
        linhas += f"""
        <tr><td style="padding:4px 0 1px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
            <td style="font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#262220;white-space:nowrap;">
              {_swatch(cor, 11)}&nbsp;{rotulo}
            </td>
            <td align="right" style="font-family:Arial,Helvetica,sans-serif;font-size:10px;color:#84796E;white-space:nowrap;padding-left:8px;">
              <b style="color:#262220;">{_fmt_moeda(valor)}</b>&nbsp;·&nbsp;{projetos}p&nbsp;·&nbsp;{_fmt_dias(dias)}
            </td>
          </tr></table>
        </td></tr>
        <tr><td style="padding:0 0 8px;">
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#EFE9DF;border-radius:3px;"><tr><td style="line-height:0;">
            <table role="presentation" cellpadding="0" cellspacing="0" width="{largura}%" style="width:{largura}%;"><tr>
              <td style="background:{cor};height:{altura_barra}px;border-radius:3px;line-height:{altura_barra}px;font-size:0;">&nbsp;</td>
            </tr></table>
          </td></tr></table>
        </td></tr>"""
    return f"""<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#FFFFFF;border:1px solid #E7E1D8;border-radius:6px;">
      <tr><td style="padding:14px 16px 0;">
        <div style="font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#201D1B;text-transform:uppercase;letter-spacing:.03em;">{titulo}</div>
        <div style="font-family:Arial,Helvetica,sans-serif;font-size:9.5px;color:#84796E;padding-top:1px;">{subtitulo}</div>
      </td></tr>
      <tr><td style="padding:4px 16px 12px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0">{linhas}
        </table>
      </td></tr>
    </table>"""


def _cards_resumo(resumo_gerencia, cor_por_gerencia):
    n = max(len(resumo_gerencia), 1)
    largura = f"{100 / n:.2f}%"
    celulas = ""
    for ger, row in resumo_gerencia.iterrows():
        cor = cor_por_gerencia[ger]
        celulas += f"""
    <td width="{largura}" valign="top" style="padding:0 6px;vertical-align:top;">
      <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#FFFFFF;border:1px solid #E7E1D8;border-top:3px solid {cor};border-radius:6px;">
        <tr><td style="padding:16px 16px 12px;">
          <div style="font-family:Arial,Helvetica,sans-serif;font-size:13px;font-weight:700;color:#201D1B;text-transform:uppercase;letter-spacing:.03em;margin-bottom:10px;">
            {_swatch(cor, 12)}&nbsp;{ger.title()}
          </div>
          <table role="presentation" width="100%" cellpadding="0" cellspacing="0">
            <tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#84796E;padding:3px 0;">Valor a receber</td></tr>
            <tr><td style="font-family:Arial,Helvetica,sans-serif;font-size:17px;font-weight:700;color:{cor};padding:0 0 8px;">{_fmt_moeda(row['VALOR'])}</td></tr>
            <tr><td style="padding:6px 0 0;border-top:1px solid #E7E1D8;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#84796E;">Projetos com pendência</td>
                <td align="right" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#262220;">{int(row['PROJETOS'])}</td>
              </tr></table>
            </td></tr>
            <tr><td style="padding:4px 0 0;">
              <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
                <td style="font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#84796E;">Média de dias</td>
                <td align="right" style="font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#262220;">{_fmt_dias(row['DIAS'])}</td>
              </tr></table>
            </td></tr>
          </table>
        </td></tr>
      </table>
    </td>"""
    return f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>{celulas}</tr></table>'


def _tabela_detalhada(df, estilo_por_gerencia, resumo_gerencia):
    linhas = """
<tr>
  <td style="padding:10px 12px;background:#201D1B;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;">Projeto</td>
  <td style="padding:10px 12px;background:#201D1B;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;">Título da obra</td>
  <td align="right" style="padding:10px 12px;background:#201D1B;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;">Valor a receber</td>
  <td align="right" style="padding:10px 12px;background:#201D1B;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.03em;">Dias pendente</td>
</tr>"""

    for ger in resumo_gerencia.index:
        estilo = estilo_por_gerencia[ger]
        df_ger = df[df['OPERACAO'] == ger]
        unidades = ', '.join(sorted(df_ger['UNIDADE'].astype(str).str.title().unique()))
        linhas += f"""
    <tr><td colspan="4" style="padding:10px 12px;background:{estilo['header_bg']};border-top:2px solid {estilo['accent']};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;color:#201D1B;text-transform:uppercase;letter-spacing:.02em;">
        {_swatch(estilo['accent'], 12)}&nbsp;{ger.title()} <span style="color:#84796E;font-weight:400;text-transform:none;">&nbsp;·&nbsp;Unidades: {unidades}</span>
    </td></tr>"""

        df_supervisores = (
            df_ger.groupby('SUPERVISOR')
            .agg(VALOR=('VALOR', 'sum'), PROJETOS=('PROJETO', 'count'), DIAS=('DIAS', 'mean'))
            .sort_values('VALOR', ascending=False)
        )

        for supervisor, srow in df_supervisores.iterrows():
            cor_media = _cor_dias(srow['DIAS'])
            linhas += f"""
    <tr><td colspan="2" style="padding:7px 12px 7px 22px;background:{estilo['subtotal_bg']};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:11.5px;font-weight:700;color:#262220;">{supervisor}</td>
      <td align="right" style="padding:7px 12px;background:{estilo['subtotal_bg']};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:11px;font-weight:700;color:#262220;">{_fmt_moeda(srow['VALOR'])}</td>
      <td align="right" style="padding:7px 12px;background:{estilo['subtotal_bg']};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:11px;color:#84796E;">{int(srow['PROJETOS'])}p &nbsp;·&nbsp; <span style="color:{cor_media};">média {_fmt_dias(srow['DIAS'])}</span></td>
    </tr>"""

            df_projetos = df_ger[df_ger['SUPERVISOR'] == supervisor].sort_values('VALOR', ascending=False)
            for j, (_, prow) in enumerate(df_projetos.iterrows()):
                bg = estilo['row_a'] if j % 2 == 0 else estilo['row_b']
                cor_dias = _cor_dias(prow['DIAS'])
                linhas += f"""
    <tr>
      <td style="padding:7px 12px 7px 22px;background:{bg};border-bottom:1px solid #E7E1D8;font-family:'Courier New',monospace;font-size:11px;color:#84796E;">{prow['PROJETO']}</td>
      <td style="padding:7px 12px;background:{bg};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:11.5px;color:#262220;">{prow['TITULO']}</td>
      <td align="right" style="padding:7px 12px;background:{bg};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:11.5px;color:#262220;">{_fmt_moeda(prow['VALOR'])}</td>
      <td align="right" style="padding:7px 12px;background:{bg};border-bottom:1px solid #E7E1D8;font-family:Arial,Helvetica,sans-serif;font-size:11.5px;"><span style="color:{cor_dias};">{_fmt_dias(prow['DIAS'])}</span></td>
    </tr>"""

    total_valor = df['VALOR'].sum()
    total_dias = df['DIAS'].mean()
    linhas += f"""
<tr>
  <td colspan="2" style="padding:11px 12px;background:#2E2A27;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;">Total geral &nbsp;<span style="font-weight:400;color:#B8ADA3;">({len(df)} projetos)</span></td>
  <td align="right" style="padding:11px 12px;background:#2E2A27;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;">{_fmt_moeda(total_valor)}</td>
  <td align="right" style="padding:11px 12px;background:#2E2A27;color:#FFFFFF;font-family:Arial,Helvetica,sans-serif;font-size:12px;font-weight:700;">{_fmt_dias(total_dias)}</td>
</tr>"""

    return f'<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="border:1px solid #E7E1D8;border-radius:6px;overflow:hidden;">{linhas}</table>'


def montar_dashboard_html(
    df,
    eyebrow,
    titulo_html,
    texto_contagem,
    escopo_titulo,
    escopo_texto,
    col_projeto='PROJETO',
    col_titulo='TITULO',
    col_valor='VALOR',
    col_dias='DIAS',
    col_unidade='UNIDADE',
    col_supervisor='SUPERVISOR',
    col_operacao='OPERACAO',
):
    """Monta o layout de dashboard (baseado em assets/dashboard_encerramento_obras_email.html)
    a partir de um DataFrame com colunas de projeto/valor/dias/unidade/supervisor/gerência.
    """
    df = df.rename(columns={
        col_projeto: 'PROJETO',
        col_titulo: 'TITULO',
        col_valor: 'VALOR',
        col_dias: 'DIAS',
        col_unidade: 'UNIDADE',
        col_supervisor: 'SUPERVISOR',
        col_operacao: 'OPERACAO',
    }).copy()
    df['OPERACAO'] = df['OPERACAO'].fillna('-')

    resumo_gerencia = (
        df.groupby('OPERACAO')
        .agg(VALOR=('VALOR', 'sum'), PROJETOS=('PROJETO', 'count'), DIAS=('DIAS', 'mean'))
        .sort_values('VALOR', ascending=False)
    )

    estilo_por_gerencia = {ger: _estilo_gerencia(ger) for ger in resumo_gerencia.index}
    cor_por_gerencia = {ger: estilo['accent'] for ger, estilo in estilo_por_gerencia.items()}

    itens_gerencia = [(ger.title(), row['VALOR'], int(row['PROJETOS']), row['DIAS']) for ger, row in resumo_gerencia.iterrows()]
    cores_gerencia = [cor_por_gerencia[g] for g in resumo_gerencia.index]

    resumo_unidade = (
        df.groupby('UNIDADE')
        .agg(VALOR=('VALOR', 'sum'), PROJETOS=('PROJETO', 'count'), DIAS=('DIAS', 'mean'), OPERACAO=('OPERACAO', 'first'))
        .sort_values('VALOR', ascending=False)
    )
    itens_unidade = [(str(unidade).title(), row['VALOR'], int(row['PROJETOS']), row['DIAS']) for unidade, row in resumo_unidade.iterrows()]
    cores_unidade = [_estilo_gerencia(row['OPERACAO'])['accent'] for _, row in resumo_unidade.iterrows()]

    resumo_supervisor = (
        df.groupby('SUPERVISOR')
        .agg(VALOR=('VALOR', 'sum'), PROJETOS=('PROJETO', 'count'), DIAS=('DIAS', 'mean'), OPERACAO=('OPERACAO', 'first'))
        .sort_values('VALOR', ascending=False)
    )
    itens_supervisor = [(supervisor, row['VALOR'], int(row['PROJETOS']), row['DIAS']) for supervisor, row in resumo_supervisor.iterrows()]
    cores_supervisor = [_estilo_gerencia(row['OPERACAO'])['accent'] for _, row in resumo_supervisor.iterrows()]

    legenda = "".join(
        f'{_swatch(cor_por_gerencia[ger], 12)}&nbsp;{ger.title()}&nbsp;&nbsp;&nbsp;'
        for ger in resumo_gerencia.index
    )

    data_atual = datetime.now().strftime('%d/%m/%Y')
    total_projetos = len(df)

    return f"""
<body style="margin:0;padding:0;background:#F7F5F2;">
<table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F7F5F2;">
<tr><td align="center" style="padding:24px 12px;">

<table role="presentation" width="680" cellpadding="0" cellspacing="0" style="width:680px;max-width:100%;background:#FFFFFF;border-radius:8px;overflow:hidden;border:1px solid #E7E1D8;">

  <tr><td style="background:#D5040C;padding:22px 26px;border-top:4px solid #F4AB00;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-family:Arial,Helvetica,sans-serif;">
        <div style="font-size:11px;color:#FFE7A8;font-weight:700;text-transform:uppercase;letter-spacing:.1em;">{eyebrow}</div>
        <div style="font-size:21px;color:#FFFFFF;font-weight:700;margin-top:6px;line-height:1.25;">{titulo_html}</div>
      </td>
    </tr></table>
  </td></tr>

  <tr><td style="padding:16px 26px 0;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-family:Arial,Helvetica,sans-serif;font-size:12px;color:#84796E;">
        Atualizado em <b style="color:#262220;">{data_atual}</b> &nbsp;·&nbsp; <b style="color:#262220;">{total_projetos}</b> {texto_contagem}
      </td>
    </tr></table>
  </td></tr>

  <tr><td style="padding:16px 20px 6px;">
    {_cards_resumo(resumo_gerencia, cor_por_gerencia)}
  </td></tr>

  <tr><td style="padding:18px 20px 0;">
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:10.5px;color:#84796E;padding:2px 0 12px;">{legenda}</div>

<table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
  <td width="33.33%" valign="top" style="padding:0 5px 0 0;vertical-align:top;">
    {_bloco_ranking('Por Gerência', f'{len(resumo_gerencia)} gerências ativas em obras', itens_gerencia, cores_gerencia, 14)}
  </td>
  <td width="33.33%" valign="top" style="padding:0 5px;vertical-align:top;">
    {_bloco_ranking('Por Operação (Unidade)', 'Ordenado por valor a receber', itens_unidade, cores_unidade, 10)}
  </td>
  <td width="33.33%" valign="top" style="padding:0 0 0 5px;vertical-align:top;">
    {_bloco_ranking('Por Supervisor', 'Ordenado por valor a receber', itens_supervisor, cores_supervisor, 7)}
  </td>
</tr></table>
  </td></tr>

  <tr><td style="padding:22px 20px 6px;">
    <div style="font-family:Arial,Helvetica,sans-serif;font-size:14px;font-weight:700;color:#201D1B;text-transform:uppercase;letter-spacing:.02em;margin-bottom:10px;">
      Detalhamento por Gerência e Supervisão
    </div>
    {_tabela_detalhada(df, estilo_por_gerencia, resumo_gerencia)}
  </td></tr>

  <tr><td style="padding:16px 26px 24px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background:#F7F5F2;border:1px solid #E7E1D8;border-radius:6px;">
      <tr><td style="padding:12px 14px;font-family:Arial,Helvetica,sans-serif;font-size:11px;line-height:1.6;color:#84796E;">
        <b style="color:#262220;">{escopo_titulo}</b>: {escopo_texto}<br>
        <b style="color:#262220;">Operação</b> = unidade operacional (município). <b style="color:#262220;">Média de dias</b>: calculada sobre os projetos com contagem de dias registrada na base de origem.
      </td></tr>
    </table>
  </td></tr>

  <tr><td style="background:#2E2A27;padding:14px 26px;">
    <table role="presentation" width="100%" cellpadding="0" cellspacing="0"><tr>
      <td style="font-family:Arial,Helvetica,sans-serif;font-size:10.5px;color:#B8ADA3;">
        Sirtec Sistemas Elétricos &nbsp;·&nbsp; Setor de Encerramento de Obras e Serviços
      </td>
    </tr></table>
  </td></tr>

</table>

</td></tr>
</table>
</body>
"""
