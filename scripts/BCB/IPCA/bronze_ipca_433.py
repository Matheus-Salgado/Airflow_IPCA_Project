from operators_ipca import upsert_ipca_bronze
import os

from dotenv import load_dotenv
load_dotenv()

file_path = os.getenv('bronze_ipca_433_file_path')
series_code = '433'

upsert_ipca_bronze(file_path, series_code)