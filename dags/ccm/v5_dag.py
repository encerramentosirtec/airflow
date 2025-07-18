#from airflow.models.dag import DAG
from airflow.sdk import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from pendulum import duration, today
from src.bots_ccm import Bots

bot = Bots(cred_file='global_brook.json')
unidades = ['BRUMADO', 'CONQUISTA', 'BARREIRAS', 'IRECE', 'JEQUIE', 'IBOTIRAMA', 'LAPA', 'GUANAMBI']

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
        #default_view="graph",
        start_date=today('America/Sao_Paulo'),
        schedule = '0 6-22 * * 1-6',
        max_active_runs = 1,
        tags = ['obra', 'geoex'],
        catchup = False) as dag:
    
    tarefas = []
    for nome in unidades:
        task = PythonOperator(
            task_id=f"{nome.lower()}",
            python_callable=bot.atualiza_pasta_v5,
            op_args=[f'OBRAS {nome}']
        )
        tarefas.append(task)
    
    for prev, next_ in zip(tarefas, tarefas[1:]):
        prev >> next_
    