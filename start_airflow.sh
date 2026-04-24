#!/bin/bash

WORKDIR="/home/sirtec-fechamento/airflow"
AIRFLOW_HOME="$WORKDIR"
VENV="$WORKDIR/airenv/bin/activate"
LOG_FILE="$WORKDIR/airflow.log"

# Ativa o ambiente virtual
source "$VENV"

# Inicia o Airflow Standalone e salva o log
#airflow standalone > "$LOG_FILE" 2>&1
airflow standalone

#echo "Airflow iniciado. Logs disponíveis em $LOG_FILE"