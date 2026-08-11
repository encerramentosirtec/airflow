import os
import requests
import base64
from dotenv import load_dotenv

load_dotenv()

class EvolutionAPI:
    def __init__(self):
        self.api_url = "http://localhost:8081"
        self.api_key = os.getenv("EVOLUTION_API_KEY")
        self.instance = "encerramento_sirtec"

    def send_text_message(self, to: str, message: str) -> dict:
        url = f"{self.api_url}/message/sendText/{self.instance}"
        headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "number": to,
            "text": message,
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=30)
        if not resp.ok:
            print("Status:", resp.status_code)
            print("Resposta:", resp.text)
        resp.raise_for_status()
        return resp.json()

    def send_image_from_file(self, to: str, file_path: str, caption: str = "") -> dict:
        with open(file_path, "rb") as f:
            media_b64 = base64.b64encode(f.read()).decode("utf-8")

        url = f"{self.api_url}/message/sendMedia/{self.instance}"
        headers = {
            "apikey": self.api_key,
            "Content-Type": "application/json",
        }
        payload = {
            "number": to,
            "mediatype": "image",
            "mimetype": "image/png",
            "caption": caption,
            "media": media_b64,
            "fileName": os.path.basename(file_path),
        }
        resp = requests.post(url, headers=headers, json=payload, timeout=60)
        resp.raise_for_status()
        return resp.json()




if __name__ == "__main__":
    evolution_api = EvolutionAPI()

    # resultado = evolution_api.send_text_message(
    #     to="5577981010127",
    #     message="Teste de integração Evolution API 🚀",
    # )
    # print(resultado)


    resultado = evolution_api.send_image_from_file(
        to="5577981010127",
        file_path="/home/hugoviana/airflow_sirtec/airflow/dashboard_exemplo.png",
        caption="Teste de envio de imagem via Evolution API 📸",
    )
    print(resultado)