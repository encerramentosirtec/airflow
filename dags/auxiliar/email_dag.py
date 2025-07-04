#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from playwright.sync_api import sync_playwright
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta
from email.mime.image import MIMEImage
from email.mime.text import MIMEText
from pendulum import today
from time import sleep
import smtplib
import os

elements = ['#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(23)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(24)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(19)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(21)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(20)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(22)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(15)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(16)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(17)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(18)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(26)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(25)',
            '#body > div.lego-reporting-view.activity-view.no-licensed.new-resizer.no-reposition > div > ng2-reporting-plate > plate > div > div > div > div:nth-child(1) > div.pancake-container > div:nth-child(2) > div > div > div > canvas-pancake-adapter > canvas-layout > div > div > div.mainBlockHolder > div > div > div > ng2-report > ng2-canvas-container > div > div:nth-child(27)',
            ]

files = ['downloads/pizza-serviços.png',
            'downloads/pizza-fotos.png',
            'downloads/barras-serviços-gerencia.png',
            'downloads/barras-fotos-gerencia.png',
            'downloads/barras-serviços-operacao.png',
            'downloads/barras-fotos-operacao.png',
            'downloads/barras-serviços-setor.png',
            'downloads/barras-fotos-setor.png',
            'downloads/linhas-serviços-gerencia.png',
            'downloads/linhas-fotos-gerencia.png',
            'downloads/equipes-sequencia.png',
            'downloads/equipes-pendencias.png',
            'downloads/linhas-pendencias.png',
            ]

link = 'https://lookerstudio.google.com/s/jIYCgVoQ5GE'
PATH = os.getenv('AIRFLOW_HOME')
ASSINATURA = """
            <br>
            <p style="font-family:Arial; font-size:12px; color:#444;">
            Atenciosamente,<br>
            <strong>Setor encerramento de obras e serviços</strong><br>
            <img src="cid:assinatura">
            </p>
        """

#tirar print dos gráficos no looker
def capturaElementos():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(link)
        sleep(60*5)
        for element, file in zip(elements, files):
            page.locator(element).screenshot(path=os.path.join(PATH,file))
        browser.close()
    
    print("imagens salvas")


def enviaEmail():

    data_atual = datetime.now() - timedelta(days=1)
    data_formatada = data_atual.strftime('%d/%m/%Y')
    dias_portugues = ['Segunda-feira',
                        'Terça-feira',
                        'Quarta-feira',
                        'Quinta-feira',
                        'Sexta-feira',
                        'Sábado',
                        'Domingo']
    nome_dia = dias_portugues[data_atual.weekday()]

    #enviar email
    # configuração
    port = 465
    smtp_server = "email-ssl.com.br"
    login = "encerramento.obraseservicos@sirtec.com.br"  # Your login
    password = "cb!Verdadef6"  # Your password

    sender_email = "encerramento.obraseservicos@sirtec.com.br"
    receiver_emails_test = ["heli.silva@sirtec.com.br"]
    receiver_emails = ["darcirs@sirtec.com.br",
                            "rodrigom@sirtec.com.br",
                            "jorge.zanette@sirtec.com.br",
                            "leonardo.luchese@sirtec.com.br",

                            "claudinei.alves@sirtec.com.br",
                            "alex.diniz@sirtec.com.br",
                            "ricardo.rodrigues@sirtec.com.br",
                            "silvio.silva@sirtec.com.br",
                            "gilberto.filho@sirtec.com.br",
                            "felippe.salles@sirtec.com.br",
                            "hugo.viana@sirtec.com.br",
                            "gabriel.brito@sirtec.com.br",
                            "larissa.sousa@sirtec.com.br",
                            "heli.silva@sirtec.com.br",
                            
                            "edson.moura@sirtec.com.br",
                            "lucas.santos@sirtec.com.br",
                            "charles.souza@sirtec.com.br",
                            "jean.palacio@sirtec.com.br",
                            "marcelo.mendonca@sirtec.com.br",
                            "leonardo.oliveira@sirtec.com.br",
                            "ednelson.braga@sirtec.com.br",
                            
                            "tainan.rodrigues@sirtec.com.br",
                            "cesar.jung@sirtec.com.br",]

    # HTML content with an image embedded
    html = f"""<html lang="pt-BR">
    <head>
        <meta charset="UTF-8">
        <meta name="viewport" content="width=device-width, initial-scale=1.0">
        <link rel="stylesheet" href="styles.css">
        <title>Relatório de Atendimentos</title>
    </head>
    <body>
        <p>Bom dia!<br><br>

        Segue relatório de atendimentos do dia {data_formatada} ({nome_dia}) registrados no GPM com pendências de Fotos e Serviços.<br>

        Para maior detalhamento acesse o dashboard: <b><a href="{link}">Clique Aqui.</a></b>
        </p>
        <p><img src="cid:image0" width="350"> <img src="cid:image1" width="350"></p>
        <p><img src="cid:image2" width="350"> <img src="cid:image3" width="350"></p>
        <p><img src="cid:image4" width="350"> <img src="cid:image5" width="350"></p>
        <p><img src="cid:image6" width="350"> <img src="cid:image7" width="350"></p>
        <p><img src="cid:image8" width="350"> <img src="cid:image9" width="350"></p>
        <p><img src="cid:image10" width="350"> <img src="cid:image11" width="350"></p>
        <p><img src="cid:image12" width="350"></p>
    </body>
    </html>"""

    # Create a multipart message and set headers
    message = MIMEMultipart()
    message["From"] = sender_email
    message["To"] = ", ".join(receiver_emails)
    #message["Bcc"] = "heli.silva@sirtec.com.br"
    message["Subject"] = "Relatório de atendimentos com pendências no GPM"

    # Attach the HTML part
    corpo_completo = f"{html}{ASSINATURA}"
    message.attach(MIMEText(corpo_completo, "html"))

    # Specify the path to your image
    image_path = files  # Change this to the correct path

    print('abre imagens')
    # Open the image file in binary mode
    for i in range(len(image_path)):
        with open(image_path[i], 'rb') as img:
            # Attach the image file
            msg_img = MIMEImage(img.read(), name=os.path.basename(image_path[i]))
            # Define the Content-ID header to use in the HTML body
            msg_img.add_header('Content-ID', f'<image{i}>')
            # Attach the image to the message
            message.attach(msg_img)

    print('envia email')
    # Send the email
    with smtplib.SMTP_SSL(smtp_server, port) as server:
        #server.starttls()
        server.login(login, password)
        server.sendmail(sender_email, receiver_emails, message.as_string())

    print('Sent')


default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'heli'
}

with DAG('email-gpm',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule_interval = '0 12 * * 1-6',
        max_active_runs = 1,
        tags = ['email', 'gpm'],
        catchup = False) as dag:
    
    captura = PythonOperator(
        task_id = 'captura',
        python_callable = capturaElementos
    )

    envia_email = PythonOperator(
        task_id = 'envia_email',
        python_callable = enviaEmail
    )

    captura>>envia_email