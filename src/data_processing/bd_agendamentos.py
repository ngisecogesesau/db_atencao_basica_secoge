import pandas as pd
import logging

from src.utils.extract_googlesheet_df import get_file_as_dataframes_google
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_bd_agendamentos():
    """
    Ler e processar dados para a tabela de 'agendamentos' a partir da aba 'Planilha1'.

    :return: Um DataFrame processado para 'agendamentos'
    """
    relative_url_agendamentos = '1q1pMYDn_KOWtur-zGL4VuvIsCkL6MnBMujUEOPkIyLw'
        
    df_agendamentos = get_file_as_dataframes_google(relative_url_agendamentos)
    df_bd_agendamentos = df_agendamentos['BD - Agendamentos']
    df_bd_agendamentos = remove_espacos_e_acentos(df_bd_agendamentos)
    df_bd_agendamentos = add_pk(df_bd_agendamentos, 'bd_agendamentos')

    df_bd_agenda_configurada = df_agendamentos['BD_Agenda_Configurada']
    df_bd_agenda_configurada = remove_espacos_e_acentos(df_bd_agenda_configurada)
    df_bd_agenda_configurada = add_pk(df_bd_agenda_configurada, 'bd_agenda_configurada')

    df_interdicoes = df_agendamentos['Interdições']
    df_interdicoes = remove_espacos_e_acentos(df_interdicoes)
    df_interdicoes = add_pk(df_interdicoes, 'interdicoes')

    return {
        'bd_agendamentos': df_bd_agendamentos,
        'bd_agenda_configurada': df_bd_agenda_configurada,
        'interdicoes': df_interdicoes
    }
