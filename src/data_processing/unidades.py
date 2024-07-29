import os
import logging
from extract_sharepoint_df import get_file_as_dataframes, log_dataframes_info, create_dataframe_variables
from ..utils.extract_sharepoint_df import get_file_as_dataframes, log_dataframes_info

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

url_usf = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
url_unidades = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/Unidades.xlsx"

dataframes_usf = get_file_as_dataframes(url_usf)
dataframes_unidades = get_file_as_dataframes(url_unidades)

dataframes_usf = log_dataframes_info(dataframes_usf)
dataframes_unidades = log_dataframes_info(dataframes_unidades)

create_dataframe_variables(dataframes_usf, 'df_usf')
create_dataframe_variables(dataframes_unidades, 'df_unidades')

df_usf_vars = [var_name for var_name in globals() if var_name.startswith('df_usf_')]
df_unidades_vars = [var_name for var_name in globals() if var_name.startswith('df_unidades_')]

for var_name in df_usf_vars:
    logger.info(f"Primeiras linhas do DataFrame '{var_name}':\n{globals()[var_name].head()}")

for var_name in df_unidades_vars:
    logger.info(f"Primeiras linhas do DataFrame '{var_name}':\n{globals()[var_name].head()}")
