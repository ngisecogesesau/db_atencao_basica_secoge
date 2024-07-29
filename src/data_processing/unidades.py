import os
import logging
from extract_sharepoint_df import get_file_as_dataframes, log_dataframes_info

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

relative_url = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
dataframes = get_file_as_dataframes(relative_url)
log_dataframes_info(dataframes)



