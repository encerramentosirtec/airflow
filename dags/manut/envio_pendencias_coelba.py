import sys
import os
import seaborn as sns


PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


import spreadsheets as sh


from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('causal_scarab.json')


PLANILHA_POSTAGEM = GS_SERVICE.le_planilha(url=sh.MANUT_POSTAGEM, aba='Junção')


def define_pendencias_cadastro():

    pendencias_cadastro = PLANILHA_POSTAGEM.query("(UAR == 'B. Informado' or UAR == 'C. Aprovado') or (`Status PRJ` == 'Em analise')")
    pendencias_cadastro = pendencias_cadastro[['Projeto', 'OC/PMS', 'UTD', 'UAR', 'PRJ', 'Status PRJ', 'Valor total']]

    map_gerencia = {
        'ITAPETINGA': 'SUDESTE',
        'CONQUISTA': 'SUDESTE',
        'LUIS EDUARDO': 'EXTREMO OESTE',
        'JEQUIÉ': 'CENTRO OESTE',
        'BARREIRAS': 'EXTREMO OESTE',
        'BRUMADO': 'SUDOESTE',
        'IBOTIRAMA': 'OESTE',
        'LAPA': 'OESTE',
        'SANTA MARIA': 'OESTE',
        'GUANAMBI': 'SUDOESTE',
        'LIVRAMENTO': 'SUDOESTE'
    }

    pendencias_cadastro['Gerência'] = pendencias_cadastro['UTD'].map(map_gerencia)

    df_fig_cadastro = pendencias_cadastro.groupby(['UTD', 'Gerência'])[['Valor total', 'OC/PMS']].agg({'Valor total': 'sum', 'OC/PMS': pd.Series.nunique}).sort_values(ascending=False, by='Valor total').reset_index()


    # Criar mapa de cores por Gerência
    gerencias = df_fig_cadastro['Gerência'].unique()
    palette = sns.color_palette('Set2', n_colors=len(gerencias))
    color_map = dict(zip(gerencias, palette))

    # Cores por barra
    colors = df_fig_cadastro['Gerência'].map(color_map)


    # Gráfico
    plt.figure(figsize=(7, 4))
    ax = sns.barplot(data=df_fig_cadastro, x='Valor total', y='UTD', orient='h', hue='Gerência', palette='Set2')


    # Adicionando rótulos nas barras
    for i, (value, qtd) in enumerate(zip(df_fig_cadastro['Valor total'], df_fig_cadastro['OC/PMS'])):
        plt.text(value + df_fig_cadastro['Valor total'].max() * 0.01,  # Pequeno deslocamento à direita da barra
                i,  # Posição vertical da barra
                f'R$ {value/1000:.1f} mil ({qtd} OCs)',
                va='center')

    # Remove moldura
    for spine in ['top', 'right', 'bottom']:
        ax.spines[spine].set_visible(False)

    # Remove eixo inferior
    ax.tick_params(axis='x', bottom=False)
    ax.set_xticklabels([])

    # Altera tamanho da fonte dos rótulos do eixo Y
    ax.tick_params(axis='y', labelsize=10)

    ax.set_xlabel('')
    plt.ylabel('UTD')
    plt.title('Pendência de finalização de cadastro por UTD', loc='left', fontweight='bold')


    # Total por Gerência
    totais_gerencia = df_fig_cadastro.groupby('Gerência')['Valor total'].sum().sort_values(ascending=False)
    texto_gerencias = '\n'.join([f"{ger}: R$ {valor/1000:,.0f} mil".replace(',', '.') for ger, valor in totais_gerencia.items()])

    # Total geral
    valor_total = df_fig_cadastro['Valor total'].sum()
    texto_total = f"TOTAL GERAL: R$ {valor_total/1000:,.0f} mil".replace(',', '.')

    # Texto final (totais)
    texto_final = texto_gerencias + '\n\n' + texto_total

    # Inserir no canto inferior direito
    plt.gcf().text(
        0.92, 0.40,  # um pouco acima (y maior que -0.35)
        "Total por Gerência",
        ha='right', va='top',
        fontsize=10, fontweight='bold', color='black'
    )

    plt.gcf().text(
        0.98, 0.35,  # mesmo Y que a legenda
        texto_final,
        ha='right', va='top',
        fontsize=9, color='black',
        bbox=dict(facecolor='white', alpha=0.8, boxstyle='round,pad=0.5')
    )

    # Mover a legenda para o canto inferior esquerdo
    ax.legend(
        title='Gerência',
        loc='lower left',
        bbox_to_anchor=(0.0, -0.45),  # mais abaixo
        frameon=False,
        ncol=2
    )

    plt.tight_layout()
    plt.savefig(os.path.join(PATH, 'assets/figures/pendencia_cadastro.png'))




def pendencias_validacao():

    pendencias_validacao = ''

    return



if __name__ == '__main__':
    define_pendencias_cadastro()