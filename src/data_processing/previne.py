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

    url_resultado_indicadores_desempenho_consolidado_ms = '/Shared Documents/SESAU/NGI/previne/Resultado Indicadores_desempenho_consolidado_MS.xlsx'
    df_resultado_indicadores_desempenho_consolidado_ms = get_file_as_dataframes(url_resultado_indicadores_desempenho_consolidado_ms)
    df_resultado_indicadores_desempenho_consolidado_ms = df_resultado_indicadores_desempenho_consolidado_ms['Consolidado DS']
    df_resultado_indicadores_desempenho_consolidado_ms = remove_espacos_e_acentos(df_resultado_indicadores_desempenho_consolidado_ms)
    df_resultado_indicadores_desempenho_consolidado_ms = add_pk(df_resultado_indicadores_desempenho_consolidado_ms, 'resultado_indicadores_desempenho_consolidado_ms')

    return {
        'serie_historica_previne': df_serie_historica_previne,
        'resultado_indicadores_desempenho_consolidado_ms': df_resultado_indicadores_desempenho_consolidado_ms
    }
