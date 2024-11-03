import pandas as pd
import os
from dotenv import load_dotenv
load_dotenv()

file_path_silver_cepea = os.getenv('silver_cepea_file_path')
file_path_gold_cepea = os.getenv('gold_cepea_file_path_csv')
file_path_gold_cepea_parquet = os.getenv('gold_cepea_file_path_parquet')

df_silver_cepea = pd.read_csv(file_path_silver_cepea)
df_gold_cepea = pd.read_csv(file_path_gold_cepea)


df_temp = pd.DataFrame({
    'dt_cmdty': df_silver_cepea['dt_indicador'],
    'nome_cmdty': 'Boi_Gordo',
    'tipo_cmdty': 'Indicador do Boi Gordo CEPEA/B3',
    'cmdty_um': '15 Kg/carcaça',
    'cmdty_vl_rs_um': df_silver_cepea['qt_valor_corrigido'],
    'dt_etl': pd.to_datetime('today').strftime('%Y-%m-%d')
})

df_gold_cepea = pd.concat([df_gold_cepea[~df_gold_cepea['dt_cmdty'].isin(df_temp['dt_cmdty'])], df_temp])


ind_nulos = df_gold_cepea['cmdty_var_mes_perc'].isnull()


df_gold_cepea.loc[ind_nulos, 'cmdty_var_mes_perc'] = (
    (df_gold_cepea['cmdty_vl_rs_um'] - df_gold_cepea['cmdty_vl_rs_um'].shift(1)) / df_gold_cepea['cmdty_vl_rs_um'].shift(1)
).loc[ind_nulos]

df_gold_cepea.to_parquet(file_path_gold_cepea_parquet, index=False)

