#!/bin/bash

WORKDIR="/home/sirtec-fechamento/airflow"
VENV="$WORKDIR/airenv/bin/activate"
LOG_FILE="$WORKDIR/airflow.log"

# Ativa o ambiente virtual
source "$VENV"

# Inicia o Airflow Standalone e salva o log
cd "$WORKDIR"
airflow standalone > "$LOG_FILE" 2>&1
