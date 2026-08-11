from google.cloud import bigquery
from google.oauth2 import service_account
import pandas as pd

import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
# os.chdir(PATH)
sys.path.insert(0, PATH)


class BigQuery:
    def __init__(self):
        credentials = service_account.Credentials.from_service_account_file(
            os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'),
            scopes=[
                "https://www.googleapis.com/auth/cloud-platform",
                "https://www.googleapis.com/auth/drive",
            ],
        )

        self.client_bigquery = bigquery.Client(
            credentials=credentials,
            project=credentials.project_id,
        )


    def overwrite_to_bigquery(self, df: pd.DataFrame, table_id: str):
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE  # sobrescreve a tabela
        )
        job = self.client_bigquery.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # espera o job terminar
        print(f"{len(df)} linhas carregadas em {table_id} (sobrescrita).")


    def append_to_bigquery(self, df: pd.DataFrame, table_id: str):
        job_config = bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND  # sobrescreve a tabela
        )
        job = self.client_bigquery.load_table_from_dataframe(df, table_id, job_config=job_config)
        job.result()  # espera o job terminar
        print(f"{len(df)} linhas carregadas em {table_id} (concatenada).")


    def query_bigquery_table(self, query):
        return self.client_bigquery.query(query).to_dataframe()
