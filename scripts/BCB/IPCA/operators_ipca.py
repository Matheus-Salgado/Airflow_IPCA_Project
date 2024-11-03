import requests
import pandas as pd
import os
from datetime import date, datetime

def full_data_bcb_ipca(series_code: str) -> pd.DataFrame:
    """
    Extrai todo o histórico de dados de uma série temporal do Banco Central do Brasil (BCB).

    Parâmetros:
    - series_code (str): Código da série temporal (e.g., '433' para o IPCA geral).

    Retorno:
    - pd.DataFrame: Dados da série temporal no formato DataFrame.
    """
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados?formato=json"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df['dt_etl'] = datetime.now().strftime('%d/%m/%Y')

        return df
    except requests.exceptions.RequestException as e:
        print("Erro na requisição da API:", e)
        return pd.DataFrame()

def incremental_data_bcb_ipca(series_code: str, start_date: str) -> pd.DataFrame:
    """
    Extrai dados de uma série temporal do Banco Central do Brasil (BCB) a partir de uma data inicial.

    Parâmetros:
    - series_code (str): Código da série temporal (e.g., '433' para o IPCA geral).
    - start_date (str): Data inicial no formato 'dd/MM/yyyy'.

    Retorno:
    - pd.DataFrame: Dados da série temporal no formato DataFrame.
    """
    end_date = datetime.now().strftime('%d/%m/%Y')
    url = f"https://api.bcb.gov.br/dados/serie/bcdata.sgs.{series_code}/dados?formato=json&dataInicial={start_date}"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()

        df = pd.DataFrame(data)
        df['dt_etl'] = datetime.now().strftime('%d/%m/%Y')

        return df
    except requests.exceptions.RequestException as e:
        print("Erro na requisição da API:", e)
        return pd.DataFrame()

def upsert_ipca_bronze(file_path: str, series_code: str):
    """
    Verifica a existência do arquivo na camada bronze e realiza a atualização ou criação de todo o histórico.

    Parâmetros:
    - file_path (str): Caminho para o arquivo na camada bronze.
    - series_code (str): Código da série temporal (e.g., '433' para o IPCA geral).
    """
    
    if os.path.exists(file_path):
        
        bronze_data = pd.read_csv(file_path)
        start_date = pd.to_datetime(bronze_data['data'], format='%d/%m/%Y').dt.date.max()

        start_date = start_date.strftime('%d/%m/%Y')

        df_incremental = incremental_data_bcb_ipca(series_code, start_date)
        
        bronze_data = bronze_data[~bronze_data['data'].isin(df_incremental['data'])]
        
        bronze_data = pd.concat([bronze_data, df_incremental], ignore_index=True)

        bronze_data.to_csv(file_path, index=False)

        print(f"Carga incremental do código {series_code} realizada com sucesso no diretório {file_path}")
    else:
        
        full_data = full_data_bcb_ipca(series_code)
        
        
        full_data.to_csv(file_path, index=False)
        
        print(f"Carga full do código {series_code} realizada com sucesso no diretório {file_path}")

