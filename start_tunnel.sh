#!/bin/bash

WORKDIR="/home/sirtec-fechamento/airflow"
VENV="$WORKDIR/airenv/bin/activate"
LOG_FILE="$WORKDIR/tunnel.log"
LINK_FILE="$WORKDIR/tunnel_link.txt"

# Ativa o ambiente virtual (se existir)
source "$VENV"

# Limpa arquivos antigos
> "$LOG_FILE"
> "$LINK_FILE"

# Inicia o túnel em background e redireciona a saída
cloudflared tunnel --url http://localhost:8080 > "$LOG_FILE" 2>&1 &

# Aguarda a geração do link e salva no arquivo
for i in {1..30}; do
    LINK=$(grep -o 'https://[^ ]*\.trycloudflare\.com' "$LOG_FILE" | head -n1)
    if [[ -n "$LINK" ]]; then
        echo "$LINK" > "$LINK_FILE"
        echo "🔗 Link do túnel: $LINK"
        break
    fi
    sleep 1
done

wait