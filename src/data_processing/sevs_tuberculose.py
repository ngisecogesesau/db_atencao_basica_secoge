import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.add_primary_key import add_pk
from src.utils.excel_operations import remove_espacos_e_acentos

def read_sevs_tuberculose():

    url_sevs_tuberculose = '/Shared Documents/SESAU/NGI/sevs_tuberculose/Tuberculose.xlsx'
    df_sevs_tuberculose = get_file_as_dataframes(url_sevs_tuberculose, sheet_name='Planilha1')
    df_sevs_tuberculose = df_sevs_tuberculose['Planilha1']
    df_sevs_tuberculose = remove_espacos_e_acentos(df_sevs_tuberculose)
    df_sevs_tuberculose = add_pk(df_sevs_tuberculose, 'sevs_tuberculose')

    return {
        'sevs_tuberculose': df_sevs_tuberculose
    }
