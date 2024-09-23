import pandas as pd
import logging

from src.utils.extract_googlesheet_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_bd_agendamentos():
    """
    Ler e processar dados para a tabela de 'agendamentos' a partir da aba 'Planilha1'.

    :return: Um DataFrame processado para 'agendamentos'
    """
    relative_url_bd_agendamentos = '1q1pMYDn_KOWtur-zGL4VuvIsCkL6MnBMujUEOPkIyLw'
        
    df_bd_agendamentos = get_file_as_dataframes(relative_url_bd_agendamentos)
    df_bd_agendamentos = df_bd_agendamentos['BD - Agendamentos']
    df_bd_agendamentos = remove_espacos_e_acentos(df_bd_agendamentos)
    df_bd_agendamentos = add_pk(df_bd_agendamentos, 'bd_agendamentos')

    df_bd_agendamentos['ds'] = df_bd_agendamentos['ds'].astype(int)

    return {
        'bd_agendamentos': df_bd_agendamentos
    }
