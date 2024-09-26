import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.add_primary_key import add_pk
from src.utils.excel_operations import remove_espacos_e_acentos

def read_sevs_licenca_sanitaria():

    url_sevs_processos_licenciamentos_sanitarios_julho = '/Shared Documents/SESAU/NGI/sevs_licenca_sanitaria/PROCESSOS LICENCIAMENTO SANITÁRIO Julho.xlsx'
    sevs_processos_licenciamentos_sanitarios_julho = get_file_as_dataframes(url_sevs_processos_licenciamentos_sanitarios_julho, sheet_name='Planilha1')
    sevs_processos_licenciamentos_sanitarios_julho = sevs_processos_licenciamentos_sanitarios_julho['Planilha1']
    sevs_processos_licenciamentos_sanitarios_julho = remove_espacos_e_acentos(sevs_processos_licenciamentos_sanitarios_julho)
    sevs_processos_licenciamentos_sanitarios_julho = add_pk(sevs_processos_licenciamentos_sanitarios_julho, 'sevs_processos_licenciamentos_sanitarios_julho')

    return {
        'sevs_processos_licenciamentos_sanitarios_julho': sevs_processos_licenciamentos_sanitarios_julho
    }
