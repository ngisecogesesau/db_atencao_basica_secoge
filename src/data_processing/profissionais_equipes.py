import os
import sys
import pandas as pd

# Ensure the root directory is in the sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_profissionais_equipes():
    """
    Ler e processar dados para 'servidores' e 'equipes'.

    :return: Um dicionário com DataFrames processados para 'servidores' e 'equipes'
    """
    relative_url_usf = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/USF.xlsx"
    relative_url_equipes_cnes = '/Shared Documents/SESAU/BI_Indicadores_Estrategicos/dados_estabelecimento_equipes_cnes_mar.xlsx'

    dataframes_usf = get_file_as_dataframes(relative_url_usf)
    dataframes_equipes_cnes = get_file_as_dataframes(relative_url_equipes_cnes)

    usf_columns = ['NOME DO SERVIDOR(A)', 'SITUAÇAO FUNCIONAL', 'PERFIL DO CARGO',  'CODIGO UNIDADE DE LOTAÇAO', 
                   'PERFIL UNIDADE DE LOTACAO', 'DISTRITO', 'EQUIPE', 'CNES UNIDADE DE LOTAÇAO', 
                    'SETOR', 'TURNO DE TRABALHO']  
    
    equipes_cnes_columns = ['CNES', 'SEQ_EQUIPE', 'DS_EQUIPE', 'NM_REFERENCIA', 'TURNO_ATEND', 'CRIACAO_EQUIPE', 'DT_DESATIVACAO', 
                            'ID_TP_EQUIPE', 'TP_EQUIPE', 'SG_EQUIPE']  

    df_servidores = dataframes_usf.get('USF')[usf_columns] if dataframes_usf.get('USF') is not None else None
    df_equipes = dataframes_equipes_cnes.get('in')[equipes_cnes_columns] if dataframes_equipes_cnes.get('in') is not None else None



    df_servidores = remove_espacos_e_acentos(df_servidores)
    df_equipes = remove_espacos_e_acentos(df_equipes)

    df_servidores = add_pk(df_servidores, 'servidores')
    df_equipes = add_pk(df_equipes, 'equipes')

    return {
        'servidores': df_servidores,
        'equipes': df_equipes
    }