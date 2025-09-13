from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account
import io
import os


class GoogleDrive:
    def __init__(self):
        self.path = os.getenv('AIRFLOW_HOME')
        self.creds = service_account.Credentials.from_service_account_file(os.path.join(self.path, 'assets/auth_google/causal_scarab.json'), scopes=['https://www.googleapis.com/auth/drive'])
        self.service = build('drive', 'v3', credentials=self.creds)
        self.folder_id = '13l2DJLNVtFZamWyNJ15eq9NlYKVgi4Ka'

    
    def listar_arquivos(self, query=None):
        """
        Lista arquivos no Google Drive com base em uma query opcional.
        """
        if query is None:
            query = f"'{self.folder_id}' in parents"
        
        results = self.service.files().list(
            q=query,
            fields="files(id, name, mimeType, modifiedTime, size)"
        ).execute()
        
        return results.get('files', [])
    
    
    def baixar_arquivo(self, nome_arquivo):
        """
        Baixa um arquivo da pasta pelo nome
        """
        query = f"'{self.folder_id}' in parents and name='{nome_arquivo}'"
        results = self.service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        files = results.get("files", [])

        if not files:
            raise FileNotFoundError(f"Arquivo '{nome_arquivo}' não encontrado na pasta do Drive.")

        file_id = files[0]["id"]
        file_name = files[0]["name"]

        destino = os.path.join(self.path, 'downloads', file_name)

        request = self.service.files().get_media(fileId=file_id)
        fh = io.FileIO(destino, "wb")
        downloader = MediaIoBaseDownload(fh, request)

        done = False
        while not done:
            status, done = downloader.next_chunk()
            if status:
                print(f"Progresso: {int(status.progress() * 100)}%")

        print(f"✅ Arquivo '{file_name}' salvo em '{destino}'")
        
        return destino
    
    

if __name__ == '__main__':
    drive = GoogleDrive()
    # d = drive.listar_arquivos("'1G02CK3TxGE4VN8ta5H283M8x_0GOEw2T' in parents")
    d = drive.listar_arquivos()
    print(d)

