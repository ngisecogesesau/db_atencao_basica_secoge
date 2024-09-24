import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.add_primary_key import add_pk
from src.utils.excel_operations import remove_espacos_e_acentos

def read_sevs():

    url_sevs_areas_cobertas_psa = '/Shared Documents/SESAU/NGI/sevs_asace/Áreas descobertas PSA.xlsx'
    df_areas_cobertas_psa = get_file_as_dataframes(url_sevs_areas_cobertas_psa, sheet_name='Planilha1',skiprows=1)
    df_areas_cobertas_psa = df_areas_cobertas_psa['Planilha1']
    df_areas_cobertas_psa = remove_espacos_e_acentos(df_areas_cobertas_psa)
    df_areas_cobertas_psa = add_pk(df_areas_cobertas_psa, 'areas_cobertas_psa')

    return {
        'areas_cobertas_psa': df_areas_cobertas_psa
    }


