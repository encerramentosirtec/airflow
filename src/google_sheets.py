import gspread
import pandas as pd
import os

class GoogleSheets:    
    def __init__(self, credentials):
        self.path = os.getenv('AIRFLOW_HOME')
        self.gs_service = gspread.service_account(os.path.join(self.path, f'assets/auth_google/{credentials}')) # Inicia o serviço do google sheets


    def le_planilha(self, url, aba, intervalo=None, render_option='UNFORMATTED_VALUE', dtype=None):
        """
            Obtém dados de planilha.
        """
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)
        ws = sh.worksheet(aba)
        
        if intervalo == None:
            df = ws.get_all_records(value_render_option=render_option)

            if dtype is not None:
                df = pd.DataFrame(df, dtype=dtype)
            else:
                df = pd.DataFrame(df)

            return df
        else:
            df = ws.get_values(range_name=intervalo, value_render_option=render_option)

            if dtype is not None:
                df = pd.DataFrame(df[1:], columns=df[0], dtype=dtype)
            else:
                df = pd.DataFrame(df[1:], columns=df[0])

            return df


    def escreve_planilha(self, url, aba, df, range, input_option=''):
        """
            Atualiza planilha, em local definido, com os valores de um dataframe.
        """
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)

        try:
            ws = sh.worksheet(aba)
            ws.update(df.values.tolist(), range_name=range, value_input_option=input_option)
            return True
        except Exception as e:
            print(e)
            return e


    def atualiza_planilha(self, url, aba, df, input_option=''):
        """
            Adiciona valores ao final da planilha.
        """
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)

        try:
            ws = sh.worksheet(aba)
            ws.append_rows(df.values.tolist(), value_input_option=input_option)
            return True
        except Exception as e:
            print(e)
            return e

    def sobrescreve_planilha(self, url, aba, df, input_option='USER_ENTERED'):
        """
            Limpa e atualiza a planilha por completo.
        """
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)
        
        try:
            ws = sh.worksheet(aba)
            ws.clear()
            ws.update([df.columns.values.tolist()] + df.values.tolist(), value_input_option=input_option)
            return True
        except Exception as e:
            print(e)
            return e
        
    def limpa_intervalo(self, url, aba, range):
        """
            Limpa intervalo determinado.
        """
        try:
            sh = self.gs_service.open_by_url(url)
        except:
            sh = self.gs_service.open_by_key(url)
        
        try:
            ws = sh.worksheet(aba)
            ws.batch_clear(range)
            return True
        except Exception as e:
            print(e)
            return e


