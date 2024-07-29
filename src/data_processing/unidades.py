import os
import logging
from extract_sharepoint_df import get_file_as_dataframes

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

relative_url = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
dataframes = get_file_as_dataframes(relative_url)

if dataframes is not None:
    logger.info("DataFrames criados com sucesso!")
    for sheet_name, df in dataframes.items():
        num_rows = df.shape[0]
        logger.info(f"\nDataFrame da aba '{sheet_name}':")
        logger.info(f"Número de linhas: {num_rows}")
        logger.info(f"\nPrimeiras linhas do DataFrame:\n{df.head()}")
else:
    logger.error("Falha ao criar os DataFrames.")
