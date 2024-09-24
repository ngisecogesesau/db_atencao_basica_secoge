import pandas as pd
import os
import sys
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.extract_googlesheet_df import get_file_as_dataframes_google
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def read_coletas():

    url_coletas_postos = '/Shared Documents/SESAU/NGI/sermac_coletas/COLETA POSTOS.xlsx'
    df_coletas_postos = get_file_as_dataframes(url_coletas_postos)
    df_coletas_postos = df_coletas_postos['COLETAS24_long']
    df_coletas_postos = remove_espacos_e_acentos(df_coletas_postos)
    df_coletas_postos = add_pk(df_coletas_postos, 'coletas_postos')


    url_dados_qualidade_coleta_laboratorio_clinica = "1ACFkz-Wqt4B1v2u8-UvwBE1OpdQN4OU8t9i6APG2ZaM"
    df_dados_qualidade_coleta_laboratorio_clinica = get_file_as_dataframes_google(url_dados_qualidade_coleta_laboratorio_clinica)

    df_dados_qualidade_coleta_laboratorio_clinica = df_dados_qualidade_coleta_laboratorio_clinica['Página1']
    df_dados_qualidade_coleta_laboratorio_clinica = remove_espacos_e_acentos(df_dados_qualidade_coleta_laboratorio_clinica)
    df_dados_qualidade_coleta_laboratorio_clinica = add_pk(df_dados_qualidade_coleta_laboratorio_clinica, 'dados_qualidade_coleta_laboratorio_clinica')
    print(df_dados_qualidade_coleta_laboratorio_clinica)
 

    return {
        'coletas_postos': df_coletas_postos,
        'dados_qualidade_coleta_laboratorio_clinica': df_dados_qualidade_coleta_laboratorio_clinica
    }

read_coletas()