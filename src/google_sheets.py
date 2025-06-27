import gspread
import pandas as pd
import os

class GoogleSheets:    
    def __init__(self, credentials):
        self.path = os.getenv('AIRFLOW_HOME')
        self.gs_service = gspread.service_account(os.path.join(self.path, f'assets/auth_google/{credentials}')) # Inicia o serviço do google sheets


    def le_planilha(self, url, aba, intervalo=None, render_option='UNFORMATTED_VALUE', dtype=None):
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)
        ws = sh.worksheet(aba)
        
        if intervalo == None:
            df = ws.get_all_records(value_render_option=render_option)
        else:
            df = ws.get_all_records(range_name=intervalo, value_render_option=render_option)

        if dtype is not None:
            df = pd.DataFrame(df, dtype=dtype)
        else:
            df = pd.DataFrame(df)

        return df


    def escreve_planilha(self, url, aba, df, input_option=''):
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)
        ws = sh.worksheet(aba)
        ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option=input_option)


    def sobrescreve_planilha(self, url, aba, df, input_option=''):
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)
        ws = sh.worksheet(aba)
        ws.clear()
        ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option=input_option)