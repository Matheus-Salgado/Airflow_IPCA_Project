import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

file_path_bronze_ipca = os.getenv('bronze_ipca_433_file_path')
file_path_bronze_cepea = os.getenv('bronze_cepea_file_path')
file_path_silver_cepea = os.getenv('silver_cepea_file_path')

nm_colunas = ['Data', 'À vista R$']

df_cepea = pd.read_excel(file_path_bronze_cepea, skiprows=3, usecols=nm_colunas)

df_cepea['Data'] = pd.to_datetime(df_cepea['Data'], format='%m/%Y')
df_cepea['À vista R$'] = df_cepea['À vista R$'].fillna(method='ffill')
df_cepea['Data'] = df_cepea['Data'].combine_first(df_cepea['Data'].fillna(method='ffill') + pd.DateOffset(months=1))
df_cepea['Data'] = df_cepea['Data'].combine_first(df_cepea['Data'].fillna(method='bfill') - pd.DateOffset(months=1))


df_ipca = pd.read_csv(file_path_bronze_ipca)
df_ipca['data'] = pd.to_datetime(df_ipca['data'], format='%d/%m/%Y')


df_silver_cepea = pd.merge(df_ipca, df_cepea, left_on='data', right_on='Data', how='left')

df_silver_cepea = df_silver_cepea[['data', 'À vista R$', 'valor']].rename(columns={
    'data': 'dt_indicador',
    'À vista R$': 'qt_valor',
    'valor': 'vl_ipca_mensal'
})

df_silver_cepea[['vl_ipca_acumulado', 'vl_ipca_acumulado_12_meses']] = pd.DataFrame({
    'vl_ipca_acumulado': df_silver_cepea['vl_ipca_mensal'].cumsum(),
    'vl_ipca_acumulado_12_meses': ((df_silver_cepea['vl_ipca_mensal'] / 100 + 1)
                                   .rolling(12)
                                   .apply(lambda x: x.prod(), raw=True) - 1) * 100
})

vl_ipca_acumulado_12_2022 = df_silver_cepea.loc[df_silver_cepea['dt_indicador'] == '2022-12-01', 'vl_ipca_acumulado'].values[0]

df_silver_cepea['qt_valor_corrigido'] = df_silver_cepea['qt_valor'] * (1 + (vl_ipca_acumulado_12_2022 - df_silver_cepea['vl_ipca_acumulado']) / 100)

df_silver_cepea = df_silver_cepea.loc[df_silver_cepea['qt_valor'].notnull()]

df_silver_cepea.to_csv(file_path_silver_cepea, index=False)
