#!/bin/bash

WORKDIR="/home/encerramento/airflow"
VENV="$WORKDIR/venv/bin/activate"
LOG_FILE="$WORKDIR/tunnel.log"
LINK_FILE="$WORKDIR/tunnel_link.txt"

# Ativa o ambiente virtual
source "$VENV"

# Limpa arquivos antigos
> "$LOG_FILE"
> "$LINK_FILE"

# Inicia o cloudflared diretamente (sem &)
cloudflared tunnel --url http://localhost:8080 > "$LOG_FILE" 2>&1 &

# Salva PID para referência futura
TUNNEL_PID=$!

# Obtém o IP da rede local
LOCAL_IP=$(hostname -I | awk '{print $1}')
LOCAL_URL="http://${LOCAL_IP}:8080"

# Aguarda o link ser gerado
for i in {1..30}; do
    LINK=$(grep -o 'https://[^ ]*\.trycloudflare\.com' "$LOG_FILE" | head -n1)
    if [[ -n "$LINK" ]]; then
        echo "Link do túnel: $LINK" > "$LINK_FILE"
        echo "🔗 Link do túnel: $LINK"
        if [[ -n "$LOCAL_IP" ]]; then
            echo "🌐 IP da rede local: $LOCAL_URL"
            echo "IP da rede local: $LOCAL_URL" >> "$LINK_FILE"
        fi
        break
    fi
    sleep 1
done

# Aguarda o cloudflared terminar (mantém o systemd ativo)
wait $TUNNEL_PID
