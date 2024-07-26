import os
import logging
from extract_sharepoint_df import download_file, get_file_as_dataframes


logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

relative_url = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/Atendimentos.xlsx"
local_path = "Atendimentos.xlsx"

download_file(relative_url, local_path)

dataframes = get_file_as_dataframes(local_path)

if dataframes is not None:
    logger.info("DataFrames criados com sucesso!")
    for sheet_name, df in dataframes.items():
        num_rows = df.shape[0]
        logger.info(f"\nDataFrame da aba '{sheet_name}':")
        logger.info(f"Número de linhas: {num_rows}")
else:
    logger.error("Falha ao criar os DataFrames.")
