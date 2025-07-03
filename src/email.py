import smtplib
from email.mime.multipart import MIMEMultipart
from os.path import basename
from email.mime.text import MIMEText
from email.mime.image import MIMEImage
from email.mime.application import MIMEApplication
from pathlib import Path
import os
import sys

PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

def enviaEmail(assunto, corpo_email, enviar_para, anexos, imagens_corpo_email=None, copiar=None, copia_oculta=None):
        
        """
            Constroi e envia e-mail usando e-mail padrão do setor de fechamento.

            imagens_corpo_email: Devem ser informados em forma de lista os caminhos do diretório raiz para a imagem. 
                                 No corpo do e-mail as imagens deve seguir o padrão "img_0" seguindo a ordem em que são inseridas no texto.

            copiar: Caso exista, deve ser informado em forma de lista os e-mails a serem copiados

            copia_oculta: Caso exista, deve ser informado em forma de lista os e-mails a serem copiados
        """
        
        # configuração
        porta = 465
        servidor = "email-ssl.com.br"
        login = "encerramento.obraseservicos@sirtec.com.br"
        senha = "cb!Verdadef6"
        remetente = "encerramento.obraseservicos@sirtec.com.br"


        # Elabora a mensagem
        msg = MIMEMultipart('related')
        msg['Subject'] = assunto
        msg['From'] = remetente
        msg['To'] = ', '.join(enviar_para)
        if copiar:
            msg['Cc'] = ', '.join(copiar)
        
        if copia_oculta and copiar:
            # BCC não vai no cabeçalho, só no envio
            destinatarios = enviar_para + copiar + copia_oculta
        elif copiar:
            destinatarios = enviar_para + copiar
        else:
            destinatarios = enviar_para

        msg_alternative = MIMEMultipart('alternative')
        msg.attach(msg_alternative)

        msg_alternative.attach(MIMEText(corpo_email, "html"))

        if imagens_corpo_email:
            for i, img_path in enumerate(imagens_corpo_email):
                print(f'<img_{i}>')
                print(os.path.join(PATH, img_path))

                with open(os.path.join(PATH, img_path), 'rb') as img:
                    msg_img = MIMEImage(img.read())
                    msg_img.add_header('Content-ID', f'<img_0>')
                    msg_img.add_header('Content-Disposition', 'inline', filename=os.path.basename(os.path.join(PATH,img_path)))
                    msg.attach(msg_img)



        # Adiciona anexos
        for anexo in anexos:
            path = Path(anexo)
            if path.exists():
                with open(path, 'rb') as f:
                    file_data = f.read()
                    msg.add_attachment(
                        file_data,
                        maintype='application',
                        subtype='octet-stream',
                        filename=path.name
                    )


        # Envia e-mail
        with smtplib.SMTP_SSL(servidor, porta) as smtp:
            smtp.login(login, senha)
            smtp.sendmail(remetente, destinatarios, msg.as_string())


        return ('Sent')