from airflow.models.dag import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pendulum import timezone, duration, today
from src.bots_ccm import Bots

bot = Bots()

def conquista():
    bot.atualiza_aba_v2('OBRAS CONQUISTA')
def barreiras():
    bot.atualiza_aba_v2('OBRAS BARREIRAS')
def irece():
    bot.atualiza_aba_v2('OBRAS IRECE')
def brumado():
    bot.atualiza_aba_v2('OBRAS BRUMADO')
def jequie():
    bot.atualiza_aba_v2('OBRAS JEQUIE')
def ibotirama():
    bot.atualiza_aba_v2('OBRAS IBOTIRAMA')
def lapa():
    bot.atualiza_aba_v2('OBRAS LAPA')
def guanambi():
    bot.atualiza_aba_v2('OBRAS GUANAMBI')

br_tz = timezone("Brazil/East")

default_args = {
    'depends_on_past' : False,
    'email' : ['heli.silva@sirtec.com.br'],
    'email_on_failure' : True,
    'email_on_retry' : False,
    'owner' : 'bob',
    'retries' : 2,
    'retry_delay' : duration(seconds=5)
}

with DAG('v2_atesto',
        default_args = default_args,
        default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0,30 2,6,7,8,9,10,11,13,15,16,17,19,21 * * 1-6',
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
    
    atualiza_data = PythonOperator(
        task_id='atualiza_data',
        python_callable=bot.atualiza_data_v2
    )
    
    brumado>>conquista>>barreiras>>irece>>jequie>>ibotirama>>lapa>>guanambi>>atualiza_data