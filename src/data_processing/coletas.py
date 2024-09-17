import os
import sys
import pandas as pd

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)


def read_coletas():

    url_coletas_postos = '/Shared Documents/SESAU/NGI/sermac_coletas/COLETA POSTOS.xlsx'
    df_coletas_postos = get_file_as_dataframes(url_coletas_postos)
    df_coletas_postos = df_coletas_postos['COLETAS24_long']
    df_coeltas_postos = remove_espacos_e_acentos(df_coeltas_postos)
    df_coeltas_postos = add_pk(df_coeltas_postos, 'coletas_postos')

    return {
        'coletas_postos': df_coeltas_postos
    }

