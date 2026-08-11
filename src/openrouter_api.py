import os
import requests
from dotenv import load_dotenv

load_dotenv()

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")
OPENROUTER_URL = "https://openrouter.ai/api/v1/chat/completions"

# Modelos gratuitos mudam de vez em quando no catalogo da OpenRouter.
# Verifique a lista atualizada em https://openrouter.ai/models?max_price=0
OPENROUTER_MODEL = "nvidia/nemotron-3-ultra-550b-a55b:free"

# Lista de fallback: se o modelo principal estiver indisponivel/sobrecarregado,
# tenta o proximo da lista antes de cair para a regra fixa.
OPENROUTER_MODELOS_FALLBACK = [
    OPENROUTER_MODEL,
    "google/gemini-2.0-flash-exp:free",
    "qwen/qwen-2.5-72b-instruct:free",
]


def _chamar_openrouter(prompt: str, max_tokens: int = 300) -> str:
    """Faz uma chamada a API da OpenRouter, tentando modelos gratuitos em cascata."""
    headers = {
        "Authorization": f"Bearer {OPENROUTER_API_KEY}",
        "Content-Type": "application/json",
        # Opcionais, mas recomendados pela OpenRouter para identificar a origem:
        "HTTP-Referer": "https://sirtec.internal",
        "X-Title": "Sirtec - Encerramento de Obras",
    }

    ultimo_erro = None
    for modelo in OPENROUTER_MODELOS_FALLBACK:
        payload = {
            "model": modelo,
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": 0.3,
        }
        try:
            resp = requests.post(OPENROUTER_URL, headers=headers, json=payload, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            return data["choices"][0]["message"]["content"].strip()
        except Exception as e:
            ultimo_erro = e
            continue

    raise ultimo_erro


if __name__ == "__main__":
    r = _chamar_openrouter("Ola!")
    print(r)