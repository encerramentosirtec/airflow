from functools import lru_cache

import pandas as pd

from src.evolution_api import EvolutionAPI
EVOLUTION_API = EvolutionAPI()

from src.bigquery import BigQuery
CLIENT_BIGQUERY = BigQuery()


@lru_cache(maxsize=1)
def _get_df_servicos():
    """Lê a base de caderno de serviços do BigQuery.<br>
    Resultado fica em cache em memória: só bate no BigQuery na primeira
    chamada dentro do processo; chamadas seguintes reusam o mesmo DataFrame.
    """
    try:
        query = """SELECT
                        CODIGO,
                        APLICACAO,
                        DETALHE,
                        VALOR_BASE
                    FROM
                        `sirtec-472112.external_tables.caderno_de_servicos`
                """
        return CLIENT_BIGQUERY.query_bigquery_table(query)
    except Exception as e:
        print(f"Erro ao ler a caderno de servicos: {e}")
        raise (e)


@lru_cache(maxsize=1)
def _get_df_materiais():
    """Lê a base de materiais do BigQuery.<br>
    Resultado fica em cache em memória: só bate no BigQuery na primeira
    chamada dentro do processo; chamadas seguintes reusam o mesmo DataFrame.
    """
    try:
        query = """SELECT
                        CODIGO,
                        CATEGORIA,
                        DETALHE,
                        NV_TENSAO,
                        VALOR
                    FROM
                        `sirtec-472112.external_tables.base_de_materiais`"""
        return CLIENT_BIGQUERY.query_bigquery_table(query)
    except Exception as e:
        print(f"Erro ao ler a base de materiais: {e}")
        raise (e)



def _check_cruzetas(df):
    """Verifica as quantidades de cruzeta do checklist (cruzeta dupla conta como 2).<br>
    Retorna uma lista de dicts (um por verificação), no formato padrão dos
    demais `_check_*`, pronta para virar linhas do DataFrame de checklist
    montado em `checklist()`.
    """
    qtd_cruz_aplicado_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico == 'EST_CRUZ_SIMPLES'")['Quantidade'].sum()
    qtd_cruz_aplicado_serv += df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico == 'EST_CRUZ_DUPLA'")['Quantidade'].sum() * 2
    qtd_cruz_mat = df.query("Categoria_material == 'CRUZETA'")['Quantidade'].sum()

    qtd_cruz_retirado_serv = df.query("Aplicacao_servico == 'RETIRAR' & Detalhe_servico == 'EST_CRUZ_SIMPLES'")['Quantidade'].sum()
    qtd_cruz_retirado_serv += df.query("Aplicacao_servico == 'RETIRAR' & Detalhe_servico == 'EST_CRUZ_DUPLA'")['Quantidade'].sum() * 2

    qtd_sucata_cruz_mat = abs(df.query("Categoria_material == 'SUCATA' & Detalhe_material == 'CRUZETA'")['Quantidade'].sum())

    return [
        {
            'Item': 'Cruzeta',
            'Verificacao': 'Retirado (serviço) x Sucata (material)',
            'Quantidade Servico': qtd_cruz_retirado_serv,
            'Quantidade Material': qtd_sucata_cruz_mat,
            'Diferenca': qtd_cruz_retirado_serv - qtd_sucata_cruz_mat,
            'Status': 'OK' if qtd_cruz_retirado_serv == qtd_sucata_cruz_mat else 'VERIFICAR',
        },
        {
            'Item': 'Cruzeta',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_cruz_aplicado_serv,
            'Quantidade Material': qtd_cruz_mat,
            'Diferenca': qtd_cruz_mat - qtd_cruz_aplicado_serv,
            'Status': 'OK' if qtd_cruz_mat == qtd_cruz_aplicado_serv else 'VERIFICAR',
        },
    ]


def _check_postes(df):
    """Verifica as quantidades de poste do checklist.<br>
    Retorna uma lista de dicts (um por verificação), no formato padrão dos
    demais `_check_*`, pronta para virar linhas do DataFrame de checklist
    montado em `checklist()`.
    """
    qtd_poste_aplicado_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico == 'POSTE'")['Quantidade'].sum()
    qtd_poste_mat = df.query("Categoria_material == 'POSTE'")['Quantidade'].sum()
    qtd_poste_retirado_serv = df.query("Aplicacao_servico == 'RETIRAR' & Detalhe_servico == 'POSTE'")['Quantidade'].sum()
    qtd_sucata_poste_mat = abs(df.query("Categoria_material == 'SUCATA' & Detalhe_material == 'POSTE'")['Quantidade'].sum())

    return [
        {
            'Item': 'Poste',
            'Verificacao': 'Retirado (serviço) x Sucata (material)',
            'Quantidade Servico': qtd_poste_retirado_serv,
            'Quantidade Material': qtd_sucata_poste_mat,
            'Diferenca': qtd_poste_retirado_serv - qtd_sucata_poste_mat,
            'Status': 'OK' if qtd_poste_retirado_serv == qtd_sucata_poste_mat else 'VERIFICAR',
        },
        {
            'Item': 'Poste',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_poste_aplicado_serv,
            'Quantidade Material': qtd_poste_mat,
            'Diferenca': qtd_poste_mat - qtd_poste_aplicado_serv,
            'Status': 'OK' if qtd_poste_mat == qtd_poste_aplicado_serv else 'VERIFICAR',
        },
    ]


def _diverge_percentual(qtd_material, qtd_servico, tolerancia=0.03):
    """Compara material x serviço com uma tolerância percentual (padrão 3%), em vez
    de exigir igualdade exata.<br>
    Se ambas as quantidades forem zero, não há divergência. Se apenas uma for zero,
    é considerada divergência.
    """
    if qtd_material == 0 and qtd_servico == 0:
        return False
    if qtd_servico == 0:
        return True
    razao = qtd_material / qtd_servico
    return razao <= (1 - tolerancia) or razao >= (1 + tolerancia)


def _check_equipamentos(df):
    """Verifica as quantidades de transformador e para-raio do checklist, e a
    coerência entre a retirada de trafo (serviço) e a sucata/recuperado gerado
    (material).<br>
    Retorna uma lista de dicts (um por verificação), no formato padrão dos
    demais `_check_*`, pronta para virar linhas do DataFrame de checklist
    montado em `checklist()`.
    """
    qtd_trafo_mono_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico == 'TRAFO MONO'")['Quantidade'].sum()
    qtd_trafo_poli_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico == 'TRAFO POLI'")['Quantidade'].sum()
    qtd_trafo_1f_mat = df.query("Categoria_material == 'EQUIPAMENTO' & Detalhe_material == 'TRAFO 1F'")['Quantidade'].sum()
    qtd_trafo_2f_mat = df.query("Categoria_material == 'EQUIPAMENTO' & Detalhe_material == 'TRAFO 2F'")['Quantidade'].sum()
    qtd_trafo_3f_mat = df.query("Categoria_material == 'EQUIPAMENTO' & Detalhe_material == 'TRAFO 3F'")['Quantidade'].sum()

    qtd_para_raio_mat = df.query("Categoria_material == 'PARA-RAIO'")['Quantidade'].sum()
    qtd_para_raio_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico.str.contains('PARA-RAIO', na=False)")['Quantidade'].sum()

    qtd_trafo_ret_serv = df.query("Aplicacao_servico == 'RETIRAR' & Detalhe_servico.str.contains('TRAFO', na=False)")['Quantidade'].sum()
    qtd_trafo_ret_mat = abs(df.query("Categoria_material in ['SUCATA', 'RECUP'] & Detalhe_material.str.contains('trafo', case=False, na=False)")['Quantidade'].sum())

    return [
        {
            'Item': 'Trafo Mono',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_trafo_mono_serv,
            'Quantidade Material': qtd_trafo_1f_mat + qtd_trafo_2f_mat,
            'Diferenca': (qtd_trafo_1f_mat + qtd_trafo_2f_mat) - qtd_trafo_mono_serv,
            'Status': 'OK' if qtd_trafo_mono_serv == qtd_trafo_1f_mat + qtd_trafo_2f_mat else 'VERIFICAR',
        },
        {
            'Item': 'Trafo Poli',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_trafo_poli_serv,
            'Quantidade Material': qtd_trafo_3f_mat,
            'Diferenca': qtd_trafo_3f_mat - qtd_trafo_poli_serv,
            'Status': 'OK' if qtd_trafo_poli_serv == qtd_trafo_3f_mat else 'VERIFICAR',
        },
        {
            'Item': 'Trafo',
            'Verificacao': 'Retirado (serviço) x Sucata/Recuperado (material)',
            'Quantidade Servico': qtd_trafo_ret_serv,
            'Quantidade Material': qtd_trafo_ret_mat,
            'Diferenca': qtd_trafo_ret_mat - qtd_trafo_ret_serv,
            'Status': 'OK' if qtd_trafo_ret_serv == qtd_trafo_ret_mat else 'VERIFICAR',
        },
        {
            'Item': 'Para-raio',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_para_raio_serv,
            'Quantidade Material': qtd_para_raio_mat,
            'Diferenca': qtd_para_raio_mat - qtd_para_raio_serv,
            'Status': 'OK' if qtd_para_raio_serv == qtd_para_raio_mat else 'VERIFICAR',
        },
    ]


def _check_cabos(df):
    """Verifica as quantidades de cabo do checklist: instalação, retirada de cabo NU
    (com sucata) e as bitolas de cabo MLP. Usa tolerância de 3% entre material e
    serviço (`_diverge_percentual`), em vez de igualdade exata, pois medições de
    cabo variam por metragem.<br>
    Retorna uma lista de dicts (um por verificação), no formato padrão dos
    demais `_check_*`, pronta para virar linhas do DataFrame de checklist
    montado em `checklist()`.
    """
    qtd_cabo_mat_inst = df.query("Categoria_material == 'CABO' & (Detalhe_material.str.contains('MT', na=False) | Detalhe_material.str.contains('BT', na=False) | Detalhe_material.str.contains('MLP', na=False))")['Quantidade'].sum()
    qtd_cabo_serv_inst = df.query(
        "Aplicacao_servico == 'INSTALAR'"
        "& Detalhe_servico.str.contains('CABO', na=False)"
        "& (Detalhe_servico.str.contains('MT', na=False) | Detalhe_servico.str.contains('BT', na=False) | Detalhe_servico.str.contains('MLP', na=False) | Detalhe_servico.str.contains('NU', na=False))"
    )['Quantidade'].sum()

    qtd_cabo_nu_serv_ret = df.query("Aplicacao_servico == 'RETIRAR' & Detalhe_servico.str.contains('CABO', na=False) & Detalhe_servico.str.contains('NU', na=False)")['Quantidade'].sum()
    qtd_cabo_nu_mat_ret = abs(df.query("Categoria_material == 'SUCATA' & Detalhe_material.str.contains('CABO', na=False) & Detalhe_material.str.contains('NU', na=False)")['Quantidade'].sum())

    qtd_mlp_ate_25_mat = df.query("Categoria_material == 'CABO' & Detalhe_material.str.contains('MLP', na=False) & Detalhe_material.str.contains('<25', na=False)")['Quantidade'].sum()
    qtd_mlp_25_70_mat = df.query("Categoria_material == 'CABO' & Detalhe_material.str.contains('MLP', na=False) & Detalhe_material.str.contains('>25', na=False)")['Quantidade'].sum()
    qtd_mlp_maior_70_mat = df.query("Categoria_material == 'CABO' & Detalhe_material.str.contains('MLP', na=False) & Detalhe_material.str.contains('>70', na=False)")['Quantidade'].sum()

    qtd_mlp_ate_25_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico.str.contains('MLP', na=False) & Detalhe_servico.str.contains('<25', na=False)")['Quantidade'].sum()
    qtd_mlp_25_70_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico.str.contains('MLP', na=False) & Detalhe_servico.str.contains('>25', na=False)")['Quantidade'].sum()
    qtd_mlp_maior_70_serv = df.query("Aplicacao_servico == 'INSTALAR' & Detalhe_servico.str.contains('MLP', na=False) & Detalhe_servico.str.contains('>70', na=False)")['Quantidade'].sum()

    return [
        {
            'Item': 'Cabo',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_cabo_serv_inst,
            'Quantidade Material': qtd_cabo_mat_inst,
            'Diferenca': qtd_cabo_mat_inst - qtd_cabo_serv_inst,
            'Status': 'VERIFICAR' if _diverge_percentual(qtd_cabo_mat_inst, qtd_cabo_serv_inst) else 'OK',
        },
        {
            'Item': 'Cabo NU',
            'Verificacao': 'Retirado (serviço) x Sucata (material)',
            'Quantidade Servico': qtd_cabo_nu_serv_ret,
            'Quantidade Material': qtd_cabo_nu_mat_ret,
            'Diferenca': qtd_cabo_nu_mat_ret - qtd_cabo_nu_serv_ret,
            'Status': 'VERIFICAR' if _diverge_percentual(qtd_cabo_nu_mat_ret, qtd_cabo_nu_serv_ret) else 'OK',
        },
        {
            'Item': 'Cabo MLP < 25',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_mlp_ate_25_serv,
            'Quantidade Material': qtd_mlp_ate_25_mat,
            'Diferenca': qtd_mlp_ate_25_mat - qtd_mlp_ate_25_serv,
            'Status': 'VERIFICAR' if _diverge_percentual(qtd_mlp_ate_25_mat, qtd_mlp_ate_25_serv) else 'OK',
        },
        {
            'Item': 'Cabo MLP 25 a 70',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_mlp_25_70_serv,
            'Quantidade Material': qtd_mlp_25_70_mat,
            'Diferenca': qtd_mlp_25_70_mat - qtd_mlp_25_70_serv,
            'Status': 'VERIFICAR' if _diverge_percentual(qtd_mlp_25_70_mat, qtd_mlp_25_70_serv) else 'OK',
        },
        {
            'Item': 'Cabo MLP > 70',
            'Verificacao': 'Instalado (material) x Aplicado (serviço)',
            'Quantidade Servico': qtd_mlp_maior_70_serv,
            'Quantidade Material': qtd_mlp_maior_70_mat,
            'Diferenca': qtd_mlp_maior_70_mat - qtd_mlp_maior_70_serv,
            'Status': 'VERIFICAR' if _diverge_percentual(qtd_mlp_maior_70_mat, qtd_mlp_maior_70_serv) else 'OK',
        },
    ]


def _check_fator_k(df):
    """Verifica se o valor do Fator K (10P, 50P ou 80P) aplicado no checklist é
    coerente com o percentual esperado sobre o valor total dos demais itens
    (materiais e serviços). Se houver mais de um lançamento de Fator K, ou um
    percentual não mapeado, o item é sinalizado para verificação manual. Se não
    houver Fator K lançado, não gera nenhuma linha de verificação.<br>
    Retorna uma lista de dicts (um por verificação), no formato padrão dos
    demais `_check_*`, pronta para virar linhas do DataFrame de checklist
    montado em `checklist()`.
    """
    percentuais_fator_k = {'10P': 0.1, '50P': 0.5, '80P': 0.8}

    df_fator = df.query("Aplicacao_servico == 'FATOR'")
    valor_fator = df_fator['Valor_total'].sum()
    if valor_fator == 0:
        return []
    
    valor_demais_itens = df.query("Aplicacao_servico != 'FATOR' & Grupo == 'SERVIÇOS'")['Valor_total'].sum()
    if df_fator.shape[0] != 1:
        status = 'VERIFICAR'
        valor_esperado = None
    else:
        percentual = percentuais_fator_k.get(df_fator['Detalhe_servico'].iloc[0])
        if percentual is None:
            status = 'VERIFICAR'
            valor_esperado = None
        else:
            valor_esperado = valor_demais_itens * percentual
            status = 'OK' if abs(valor_esperado - valor_fator) <= 0.01 else 'VERIFICAR'

    return [
        {
            'Item': 'Fator K',
            'Verificacao': 'Valor aplicado x Percentual esperado sobre os demais itens',
            'Quantidade Servico': '',
            'Quantidade Material': '',
            'Diferenca': None if valor_esperado is None else round(round(valor_fator, 2) - valor_esperado, 2),
            'Status': status,
        }
    ]


def checklist(df):
    """Função para gerar o checklist de fechamento<br>
    Inputs - df contendo: <br>
        - Grupo: Material ou Serviço<br>
        - Codigo: Código do material ou serviço<br>
        - Nome: Descrição do material ou serviço<br>
        - Quantidade: Quantidade do material ou serviço<br>
    """

    df_servicos = _get_df_servicos()
    df_materiais = _get_df_materiais()

    df = df.merge(df_servicos, how='left', left_on=['Codigo'], right_on=['CODIGO'], suffixes=('', '_servico'))
    df = df.merge(df_materiais, how='left', left_on=['Codigo'], right_on=['CODIGO'], suffixes=('', '_materiais'))

    df = df.drop(columns=['CODIGO', 'CODIGO_materiais'])\
            .rename(columns={
                'APLICACAO': 'Aplicacao_servico',
                'DETALHE': 'Detalhe_servico',
                'VALOR_BASE': 'Valor_servico',
                'CATEGORIA': 'Categoria_material',
                'NV_TENSAO': 'Nv_Tensao',
                'DETALHE_materiais': 'Detalhe_material',
                'VALOR': 'Valor_material',
            })
    # Valor unitário: usa o valor de serviço quando houver, senão o de material.
    df['Valor_total'] = df['Quantidade'] * df['Valor_servico'].fillna(df['Valor_material']).fillna(0)
    # print(df)

    resultados = []
    resultados += _check_postes(df)
    resultados += _check_cruzetas(df)
    resultados += _check_equipamentos(df)
    resultados += _check_cabos(df)
    resultados += _check_fator_k(df)

    df_checklist = pd.DataFrame(resultados)

    # Nem toda verificação preenche as mesmas colunas (ex.: Fator K usa Valor
    # Aplicado/Esperado em vez de Quantidade Servico/Material), então
    # pd.DataFrame(resultados) gera NaN nas células ausentes. NaN quebra o envio
    # para o Google Sheets (JSON não aceita NaN), então trocamos por ''.
    return df_checklist.fillna('')