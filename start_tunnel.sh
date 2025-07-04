#!/bin/bash

WORKDIR="/home/sirtec-fechamento/airflow"
VENV="$WORKDIR/airenv/bin/activate"
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

# Aguarda o link ser gerado
for i in {1..30}; do
    LINK=$(grep -o 'https://[^ ]*\.trycloudflare\.com' "$LOG_FILE" | head -n1)
    if [[ -n "$LINK" ]]; then
        echo "$LINK" > "$LINK_FILE"
        echo "🔗 Link do túnel: $LINK"
        break
    fi
    sleep 1
done

# Aguarda o cloudflared terminar (mantém o systemd ativo)
wait $TUNNEL_PID
