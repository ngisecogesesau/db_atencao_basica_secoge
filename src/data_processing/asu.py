import os
import sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_asu():

    #asu_monitora
    relative_url_asu_monitora = '/Shared Documents/SESAU/BI_Indicadores_Estrategicos/asu_monitora.xlsx'
    df_asu_monitora = get_file_as_dataframes(relative_url_asu_monitora)

    asu_monitora_columns = ['mes', 'ine', 'resposta', 'tipo_resposta', 'total_respostas']
    df_asu_monitora = df_asu_monitora.get('in')[asu_monitora_columns] if df_asu_monitora.get('in') is not None else None
    df_asu_monitora = remove_espacos_e_acentos(df_asu_monitora)
    df_asu_monitora = add_pk(df_asu_monitora, 'asu_monitora')

    # asu_classificacao
    relative_url_asu_classificacao = '/Shared Documents/SESAU/BI_Indicadores_Estrategicos/tooltip_classificacao_asu.xlsx'
    df_asu_classificacao = get_file_as_dataframes(relative_url_asu_classificacao)

    asu_classificacao_columns = ['Atributo', 'valor', 'numero']
    df_asu_classificacao = df_asu_classificacao.get('Planilha1')[asu_classificacao_columns] if df_asu_classificacao.get('Planilha1') is not None else None
    df_asu_classificacao = remove_espacos_e_acentos(df_asu_classificacao)
    df_asu_classificacao = add_pk(df_asu_classificacao, 'asu_classificacao')

    return {
        'asu_monitora': df_asu_monitora,
        'asu_classificacao': df_asu_classificacao
    }