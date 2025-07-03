import sys
import os
import seaborn as sns
import numpy as np
from datetime import datetime
import pendulum

PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns


from src import spreadsheets as sh
from src.email import enviaEmail

from src.google_sheets import GoogleSheets
GS_SERVICE = GoogleSheets('causal_scarab.json')


PLANILHA_POSTAGEM = GS_SERVICE.le_planilha(url=sh.MANUT_POSTAGEM, aba='Junção')

MAP_GERENCIA = {
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

def cria_grafico_valores(titulo, df):

    # Gráfico
    plt.figure(figsize=(7, 4))
    ax = sns.barplot(data=df, x='Valor total', y='UTD', orient='h', hue='Gerência', palette='Set2')


    # Adicionando rótulos nas barras
    for i, (value, qtd) in enumerate(zip(df['Valor total'], df['OC/PMS'])):
        plt.text(value + df['Valor total'].max() * 0.01,  # Pequeno deslocamento à direita da barra
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
    plt.title(titulo, loc='left', fontweight='bold')


    # Total por Gerência
    totais_gerencia = df.groupby('Gerência')['Valor total'].sum().sort_values(ascending=False)
    texto_gerencias = '\n'.join([f"{ger}: R$ {valor/1000:,.0f} mil".replace(',', '.') for ger, valor in totais_gerencia.items()])

    # Total geral
    valor_total = df['Valor total'].sum()
    texto_total = f"TOTAL GERAL: R$ {valor_total/1000:,.0f} mil".replace(',', '.')

    # Texto final (totais)
    texto_final = texto_gerencias + '\n\n' + texto_total

    # Inserir no canto inferior direito
    plt.gcf().text(
        0.92, 0.30,  # um pouco acima (y maior que -0.35)
        "Total por Gerência",
        ha='right', va='top',
        fontsize=10, fontweight='bold', color='black'
    )

    plt.gcf().text(
        0.98, 0.25,  # mesmo Y que a legenda
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
        # frameon=False,
        ncol=2
    )
    plt.subplots_adjust(bottom=0.25)  # Ajusta padding da parte inferior do gráfico
    plt.tight_layout()
    plt.savefig(os.path.join(PATH, f'assets/figures/{titulo}.png'), format='png', bbox_inches="tight")

    return


def define_pendencias_cadastro():
    """
        Gera a imagem com o gráfico e a planilha com as pendências de finalização de cadastro para serem enviados por e-mail
    """

    pendencias_cadastro = PLANILHA_POSTAGEM.query("((UAR == 'B. Informado' or UAR == 'C. Aprovado') or (`Status PRJ` == 'Em analise')) and (`Categoria de pagamento`.str.startswith('CAPEX'))")
    pendencias_cadastro.drop_duplicates(subset='OC/PMS', inplace=True)
    pendencias_cadastro = pendencias_cadastro[['Projeto', 'OC/PMS', 'UTD', 'UAR', 'PRJ', 'Status PRJ', 'Valor total']]

    pendencias_cadastro.to_excel(os.path.join(PATH, "assets/planilhas/OCs pendentes de finalização do cadastro.xlsx"), index=False)



    pendencias_cadastro['Gerência'] = pendencias_cadastro['UTD'].map(MAP_GERENCIA)

    df_fig_cadastro = pendencias_cadastro.groupby(['UTD', 'Gerência'])[['Valor total', 'OC/PMS']].agg({'Valor total': 'sum', 'OC/PMS': pd.Series.nunique}).sort_values(ascending=False, by='Valor total').reset_index()

    cria_grafico_valores("OCs pendentes de finalização do cadastro", df_fig_cadastro)



def define_pendencias_validacao():
    """
        Gera a imagem com o gráfico e a planilha com as pendências de validação da UTD para serem enviados por e-mail
    """

    query = (
        "(`Status pasta Geoex` == 'CRIADO' or "
        "`Status pasta Geoex` == 'VALIDDO') and "
        "(`Estágio da pasta` == 'K. Pendente eliminar reserva lixo (Coelba - NPPM)' or "
        "`Estágio da pasta` == 'L. Pendente de energização do projeto no SAP. (Coelba - NPPM)' or "
        "`Estágio da pasta` == 'N. Pendente de validação do HUB (Coelba - UTD)' or "
        "`Estágio da pasta` == 'O. Pendente de conciliação (Coelba - UTD)' or "
        "`Estágio da pasta` == 'P. Pasta enviada, pendente validação da pasta (Coelba - UTD)')"
    )

    pendencias_validacao = PLANILHA_POSTAGEM.query(query)[['UTD', 'Projeto', 'OC/PMS', 'Data de envio da pasta', 'Status Héktor', 'ID HRO', 'Status HRO', 'ID envio de pasta', 'Status pasta Geoex', 'Valor total']]
                                                                    
    pendencias_validacao['Gerência'] = pendencias_validacao['UTD'].map(MAP_GERENCIA)

    pendencias_validacao.to_excel(os.path.join(PATH, "assets/planilhas/Pendências de validação.xlsx"), index=False)

    df_fig_validacao = pendencias_validacao.groupby(['UTD', 'Gerência'])[['Valor total', 'OC/PMS']].agg({'Valor total': 'sum', 'OC/PMS': pd.Series.nunique}).sort_values(ascending=False, by='Valor total').reset_index()

    cria_grafico_valores("Validações pendentes", df_fig_validacao)


    # Calcula os prazos das pendências
    pendencias_validacao['Data de envio da pasta'] = pd.to_datetime(pendencias_validacao['Data de envio da pasta'], errors='coerce', dayfirst=True)

    pendencias_validacao['Dias pendentes'] = pendencias_validacao['Data de envio da pasta'].apply(
        lambda x: np.busday_count(x.date(), datetime.today().date()) if pd.notnull(x) else np.nan
    )

    df_fig_prazos = pendencias_validacao.groupby(['UTD', 'Gerência'], as_index=False)['Dias pendentes'].mean().sort_values('Dias pendentes', ascending=False)



    ### Cria gráfico dos pazos
    plt.figure(figsize=(7, 4))
    ax = sns.barplot(data=df_fig_prazos, x='Dias pendentes', y='UTD', orient='h', hue='Gerência', palette='Set2')

    # Adicionando rótulos nas barras
    for i, dias in enumerate(df_fig_prazos['Dias pendentes']):
        plt.text(dias + df_fig_prazos['Dias pendentes'].max() * 0.01,  # Pequeno deslocamento à direita da barra
                i,  # Posição vertical da barra
                f'{dias:.0f} dias',
                va='center')


   # Mover a legenda para o canto inferior esquerdo
    plt.legend(
        title="Gerência",
        loc="lower center",
        bbox_to_anchor=(0.5, -0.35),  # mais próximo do gráfico
        ncol=3,
        frameon=True,
        title_fontsize='medium'
    )

    plt.subplots_adjust(bottom=0.25)  # Ajusta padding da parte inferior do gráfico


    # Remove moldura
    for spine in ['top', 'right', 'bottom']:
        ax.spines[spine].set_visible(False)
    
    ax.set_xlabel('') # Remove nome do eixo X
    
    # Remove eixo inferior
    ax.tick_params(axis='x', bottom=False)
    ax.set_xticklabels([])

    plt.title("Médias de dias pendentes de validação", loc='left', fontweight='bold')
    plt.tight_layout()
    plt.savefig(os.path.join(PATH, f'assets/figures/Prazos validação.png'), format='png', bbox_inches="tight")



def teste_consistencia():
    """
        Faz verificação nos dados buscando alguma inconsistência para não enviar e-mail com informações incorretas.
    """

    if PLANILHA_POSTAGEM['Valor total'].sum() == 0:
        return True
    
    

    
def elabora_email():
    """
        Função para anexar os arquivos, definir o texto e enviar o email.
    """

    print('Calculando valores...')
    define_pendencias_validacao()
    define_pendencias_cadastro()

    print('Levantando e-mails...')
    contatos = GS_SERVICE.le_planilha(sh.CONTATOS, 'Contatos')
    
    # contatos_coelba = contatos.query("(Operação == 'Salvador' or Operação == 'Barreiras' or Operação == 'Luiz Eduardo Magalhães' or Operação == 'Ibotirama' or Operação == 'Bom Jesus da Lapa' or Operação == 'Santa Maria da Vitória' or Operação == 'Guanambi' or Operação == 'Vitória da Conquista' or Operação == 'Jequié' or Operação == 'Itapetinga' or Operação == 'Brumado' or Operação == 'Livramento') and (Setor == 'UTD' or Setor == 'UTEP' or Setor == 'NPPM') and (Função == 'Técnico' or Função == 'Supervisor' or Função == 'Engenheiro' or Função == 'Gerente') and (Empresa == 'Coelba')")['E-mail'].to_list()

    contatos_coelba = contatos.query("(Empresa == 'Coelba') and (Operação != 'Irecê')")['E-mail'].tolist()
    
    contatos_sirtec = contatos.query("(Empresa == 'Sirtec') and ( (Setor == 'Encerramento' and  (Subsetor == 'Manut' or Função == 'Gerente')) or (Setor == 'Operação' and Operação != 'Irecê') ) ")['E-mail'].to_list()

    todos_contatos = contatos_sirtec + contatos_coelba


    enviar_para = ["hugo.viana@sirtec.com.br"]
    # enviar_para = todos_contatos

    anexos=[
        'assets/planilhas/OCs pendentes de finalização do cadastro.xlsx',
        'assets/planilhas/Pendências de validação.xlsx'
    ]

    imagens=[
        'assets/figures/Prazos validação.png', 
        'assets/figures/Validações pendentes.png', 
        'assets/figures/OCs pendentes de finalização do cadastro.png'
    ]

    corpo= f"""
    
        <html lang="pt-BR">
        <head>
            <meta charset="UTF-8">
            <meta name="viewport" content="width=device-width, initial-scale=1.0">
        </head>
        <body>
            <p><b>Relatório de pendências Coelba</b></p>

            <p>Este relatório disponibiliza informações referentes as pendências dos projetos de manutenção que temos em tratativa atualmente.<br>
             A fim de melhorar a troca de informações entre Sirtec e Coelba, ele será enviado diariamente para termos informação em tempo real.</p>

            <p><b>1. Pendências de validação</b></p>
            <p>Relação de OCs que possuem pendência de validação da pasta ou validação de HRO ou conciliação do projeto</p>
            <p><img src="cid:img_0" "> <img src="cid:img_1" "></p><br>
            <p><b>2. Pendências de finalização de cadastro</b></p>
            <p>Relação das pastas OCs que possuem pendência de finalização de PRJ ou UAR</p>
            <p><img src="cid:img_2" "><br>
            <p>Segue anexo as bases com as OCs em questão</p>

        </body>
        </html>
        
        """
        
    print('Elaborando e enviando e-mail...')
    enviaEmail("Relatório pendencias - Manutenção Sirtec", corpo, enviar_para, anexos, imagens_corpo_email=imagens)




if __name__ == '__main__':

    elabora_email()
    sys.exit()

default_args = {
    'depends_on_past' : False,
    'email' : ['hugo.viana@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'hugo',
    'retries' : 3,
    'retry_delay' : pendulum.duration(seconds=5)
}

with DAG(
    'envia_email_pendencias_manut',
    schedule='0 10 * * 1-5',
    start_date=pendulum.today('America/Sao_Paulo'),
    catchup=False,
    max_active_runs = 1,
    tags=['manut', 'email'],
    default_args=default_args
):
    
    envia_email = PythonOperator(
        task_id='envia_email_pendencias',
        python_callable=elabora_email
    )

    envia_email