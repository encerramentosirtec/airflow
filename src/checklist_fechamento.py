

def checklist():
    # Lendo planilhas de materiais e serviços
    df_materiais = ler_planilha_google('https://docs.google.com/spreadsheets/d/18v5-EGH-l-ksblRP5Ewyh_W9NDMDytPj-zaUMHEZaI8', 'Materiais')
    df_materiais['Material'] = df_materiais['Material'].astype(str)

    df_servicos = pd.read_csv('https://docs.google.com/spreadsheets/d/e/2PACX-1vRRoenA8_io1rQKKm-z4BMLuOTiIC6_bIeGhb64tJ2MTnUxs2Z_QY7V_im8PP4tFJKjwKfYFOlZB0ux/pub?gid=0&single=true&output=tsv', sep='\t', encoding='UTF-8', decimal=',', thousands='.')

    