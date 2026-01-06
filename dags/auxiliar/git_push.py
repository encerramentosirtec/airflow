from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from pendulum import today

# Configurações
REPO_URL = "github.com/seu-usuario/seu-repositorio.git"
GIT_TOKEN = "{{ var.value.git_token }}"
LOCAL_PATH = "/tmp/meu_projeto_git"

with DAG(
    'executar_git_pull',
    start_date=today('America/Sao_Paulo'),
    schedule_interval=None,
    tags = ['git', 'bash', 'aux'],
    catchup=False ) as dag:

    sync_repo = BashOperator(
        task_id='sync_repo',
        bash_command=f"""
            cd ~/airflow
            git push
        """
    )

    sync_repo