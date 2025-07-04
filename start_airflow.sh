#!/bin/bash

WORKDIR="/home/sirtec-fechamento/airflow"
VENV="$WORKDIR/airenv/bin/activate"
LOG_FILE="$WORKDIR/airflow.log"

# Ativa o ambiente virtual (se existir)
source "$VENV"

# Limpa arquivos antigos
> "$LOG_FILE"

# Inicia o airflow em background e redireciona a saída
airflow standalone > "$LOG_FILE" 2>&1 &