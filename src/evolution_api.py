import requests
import base64

# Configuração da sua instância
API_URL = "http://192.168.3.3:8081"
API_KEY = "LQNNCJGYD8F9IWMN1L5ES9UV6XFH7NNO"
INSTANCE = "teste"

def send_text_message(to: str, message: str) -> dict:
    url = f"{API_URL}/message/sendText/{INSTANCE}"
    headers = {
        "apikey": API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "number": to,
        "text": message,
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=30)
    resp.raise_for_status() 
    return resp.json()



def send_image_from_file(to: str, file_path: str, caption: str = "") -> dict:
    with open(file_path, "rb") as f:
        media_b64 = base64.b64encode(f.read()).decode("utf-8")

    url = f"{API_URL}/message/sendMedia/{INSTANCE}"
    headers = {
        "apikey": API_KEY,
        "Content-Type": "application/json",
    }
    payload = {
        "number": to,  
        "mediatype": "image",
        "mimetype": "image/png",
        "caption": caption,
        "media": media_b64,
        "fileName": "img.png",
    }
    resp = requests.post(url, headers=headers, json=payload, timeout=60)
    resp.raise_for_status()
    return resp.json()




if __name__ == "__main__":
    resultado = send_text_message(
        to="5577981010127",
        message="Teste de integração Evolution API 🚀",
    )
    print(resultado)


    # resultado = send_image_from_file(
    #     to="5577981010127",
    #     file_path="/home/hugoviana/airflow_sirtec/airflow/assets/figures/assinatura_email.png",
    #     caption="Teste de envio de imagem via Evolution API 📸",
    # )
    # print(resultado)