import os
import sys

# Ensure the root directory is in the sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos

def read_profissionais_equipes():
    """
    Read and process data for 'servidores' and 'equipes'.

    :return: A dictionary with processed DataFrames for 'servidores' and 'equipes'
    """
    relative_url_usf = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/USF.xlsx"
    relative_url_equipes_cnes = '/Shared Documents/SESAU/BI_Indicadores_Estrategicos/EQUIPES_CNES.xlsx'
    
    dataframes_usf = get_file_as_dataframes(relative_url_usf)
    dataframes_equipes_cnes = get_file_as_dataframes(relative_url_equipes_cnes)

    # Process the DataFrames for the specific sheets
    df_servidores = remove_espacos_e_acentos(dataframes_usf['USF'])
    df_equipes = remove_espacos_e_acentos(dataframes_equipes_cnes['EQUIPES_CNES'])

    return {
        'servidores': df_servidores,
        'equipes': df_equipes
    }

