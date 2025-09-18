from google.cloud import bigquery
import pandas as pd

import os
import sys
PATH = os.getenv('AIRFLOW_HOME')
# os.chdir(PATH)
sys.path.insert(0, PATH)


class BigQuery:
    def __init__(self):
        self.client_bigquery = bigquery.Client.from_service_account_json(os.path.join(PATH, 'assets/auth_google/sirtec-bot.json'))

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


