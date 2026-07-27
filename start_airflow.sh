cd /home/encerramento/airflow

# Cria variavel de ambiente do homo do airflow
export AIRFLOW_HOME=/home/encerramento/airflow

export PYTHONPATH=/home/encerramento/airflow

# Ativa o ambiente virtual
source venv/bin/activate

# Inicia o Airflow Standalone
airflow standalone

#echo "Airflow iniciado. Logs disponíveis em $LOG_FILE"
