from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from datetime import datetime
from pendulum import timezone, duration
from BOB_V2.Commands.bib_geoex import atualiza_pasta

def conquista():
    atualiza_pasta('OBRAS CONQUISTA')
def barreiras():
    atualiza_pasta('OBRAS BARREIRAS')
def irece():
    atualiza_pasta('OBRAS IRECE')
def brumado():
    atualiza_pasta('OBRAS BRUMADO')
def jequie():
    atualiza_pasta('OBRAS JEQUIE')
def ibotirama():
    atualiza_pasta('OBRAS IBOTIRAMA')
def lapa():
    atualiza_pasta('OBRAS LAPA')
def guanambi():
    atualiza_pasta('OBRAS GUANAMBI')

br_tz = timezone("Brazil/East")

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'bob',
    'retries' : 2,
    'retry_delay' : duration(seconds=30)
}

with DAG('v5',
        default_args = default_args,
        default_view="graph",
        start_date=datetime(2024,12,1,tzinfo=br_tz),
        schedule_interval = '10 2,6,7,9,11,13,15,16,17,19,21 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    conquista = PythonOperator(
        task_id='conquista',
        python_callable=conquista
    )
    
    barreiras = PythonOperator(
        task_id='barreiras',
        python_callable=barreiras
    )
    
    irece = PythonOperator(
        task_id='irece',
        python_callable=irece
    )
    
    brumado = PythonOperator(
        task_id='brumado',
        python_callable=brumado
    )
    
    jequie = PythonOperator(
        task_id='jequie',
        python_callable=jequie
    )
    
    ibotirama = PythonOperator(
        task_id='ibotirama',
        python_callable=ibotirama
    )
    
    lapa = PythonOperator(
        task_id='lapa',
        python_callable=lapa
    )
    
    guanambi = PythonOperator(
        task_id='guanambi',
        python_callable=guanambi
    )
    
    brumado>>conquista>>barreiras>>irece>>jequie>>ibotirama>>lapa>>guanambi
    