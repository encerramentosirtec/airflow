from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
import requests

import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
os.chdir(PATH)
sys.path.insert(0, PATH)

from src.config import configs as cfg
COOKIE_GPM = cfg.cookie_gpm


def ping():

    header = {
        'cookie': COOKIE_GPM
    }
    r = requests.post('https://sirtecba.gpm.srv.br/menu.php?sis=1000', headers = header)
    if r.status_code == 200:
        print("PING GPM!")
    else:
        print("Falha:", r.status_code)


if __name__ == '__main__':
    ping()

with DAG(
    dag_id='ping_gpm',
    schedule='*/5 * * * *',
):
    
    task = PythonOperator(
        task_id='ping_task',
        python_callable=ping,
    )

    task