#!/bin/bash

# Ativa o ambiente virtual
cd ~/airflow
source airenv/bin/activate

# Inicia o Airflow em segundo plano com log
airflow standalone > ~/airflow/log_airflow.log 2>&1 &

# Aguarda alguns segundos para garantir que o Airflow subiu
sleep 10

# Inicia o túnel Cloudflare em segundo plano com log
cloudflared tunnel --url http://localhost:8080 > ~/airflow/log_tunnel.log 2>&1 &
