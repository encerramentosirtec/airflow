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


def pendencias_cadastro():

    pendencias_cadastro = PLANILHA_POSTAGEM.query("(UAR == 'B. Informado' or UAR == 'C. Aprovado') or (`Status PRJ` == 'Em analise')")
    pendencias_cadastro = pendencias_cadastro[['Projeto', 'OC/PMS', 'UTD', 'UAR', 'PRJ', 'Status PRJ', 'Valor total']]

    map_gerencia = {
        'ITAPETINGA': 'SUDESTE',
        'CONQUISTA': 'SUDESTE',
        'LUIS EDUARDO': 'EXTREMO OESTE',
        'JEQUIÉ': 'SUDESTE',
        'BARREIRAS': 'EXTREMO OESTE',
        'BRUMADO': 'SUDOESTE',
        'IBOTIRAMA': 'OESTE',
        'LAPA': 'OESTE',
        'SANTA MARIA': 'OESTE',
        'GUANAMBI': 'OESTE',
        'LIVRAMENTO': 'SUDOESTE'
    }

    pendencias_cadastro['Gerência'] = pendencias_cadastro['UTD'].map(map_gerencia)
    
    df_fig_cadastro = pendencias_cadastro.groupby(['UTD'])[['Valor total', 'OC/PMS']].agg({'Valor total': 'sum', 'OC/PMS': pd.Series.nunique}).sort_values(ascending=False, by='Valor total')

    # Gráfico
    plt.figure(figsize=(7, 4))
    ax = sns.barplot(x=df_fig_cadastro['Valor total'], y=df_fig_cadastro.index, orient='h')

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
    plt.title('Pendência de finalização de cadastro por UTD', loc='left')

    # Soma total
    valor_total = df_fig_cadastro['Valor total'].sum()
    texto_soma = f'Total: R$ {valor_total/1000:,.0f} mil'.replace(',', '.')

    # Inserindo no canto superior direito do gráfico
    plt.text(x=ax.get_xlim()[1],  # valor máximo do eixo x#
            y=len(df_fig_cadastro) + 0.5,  # topo do gráfico
            s=texto_soma,
            ha='right', va='bottom',
            fontsize=10, fontweight='bold',
            color='black')

    plt.tight_layout()
    plt.savefig(os.path.join(PATH, 'assets/figures/pendencia_cadastro.png'))




def pendencias_validacao():

    

    return



if __name__ == '__main__':
    pendencias_cadastro()