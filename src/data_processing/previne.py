import pandas as pd

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_previne():
    url_serie_historica_previne = '/Shared Documents/SESAU/NGI/previne/Série histórica Previne 2019 2024.xlsx'
    df_serie_historica_previne = get_file_as_dataframes(url_serie_historica_previne)
    df_serie_historica_previne = df_serie_historica_previne['Página1']
    df_serie_historica_previne = remove_espacos_e_acentos(df_serie_historica_previne)
    df_serie_historica_previne = add_pk(df_serie_historica_previne, 'serie_historica_previne')

    return {
        'serie_historica_previne': df_serie_historica_previne
    }
