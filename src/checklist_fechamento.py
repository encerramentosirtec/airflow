from functools import lru_cache

import pandas as pd
from src.bigquery import BigQuery

CLIENT_BIGQUERY = BigQuery()


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
                        DETALHE
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
                        NV_TENSAO
                    FROM
                        `sirtec-472112.external_tables.base_de_materiais`"""
        return CLIENT_BIGQUERY.query_bigquery_table(query)
    except Exception as e:
        print(f"Erro ao ler a base de materiais: {e}")
        raise (e)


def checklist(df):
    """Função para gerar o checklist de fechamento<br>
    Inputs:<br>
        <t>df contendo: <br>
            - Grupo: Material ou Serviço<br>
            - Codigo: Código do material ou serviço<br>
            - Nome: Descrição do material ou serviço<br>
            - Quantidade: Quantidade do material ou serviço<br>
    Returns:
        pd.DataFrame: DataFrame contendo o checklist de fechamento
    """

    df_servicos = _get_df_servicos()
    df_materiais = _get_df_materiais()

    df = df.merge(df_servicos, how='left', left_on=['Codigo'], right_on=['CODIGO'], suffixes=('', '_servico'))
    df = df.merge(df_materiais, how='left', left_on=['Codigo'], right_on=['CODIGO'], suffixes=('', '_materiais'))

    df = df.drop(columns=['CODIGO', 'CODIGO_materiais'])\
            .rename(columns={'APLICACAO': 'Aplicacao_servico', 'DETALHE': 'Detalhe_servico', 'CATEGORIA': 'Categoria_material', 'NV_TENSAO': 'Nv_Tensao', 'DETALHE_materiais': 'Detalhe_material'})


    resultados = []
    resultados += _check_postes(df)
    resultados += _check_cruzetas(df)

    df_checklist = pd.DataFrame(resultados)
    print(df_checklist)

    return df_checklist