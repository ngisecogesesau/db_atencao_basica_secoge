import os
import sys
import pandas as pd

# Ensure the root directory is in the sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos

def read_profissionais_equipes():
    """
    Ler e processar dados para 'servidores' e 'equipes'.

    :return: Um dicionário com DataFrames processados para 'servidores' e 'equipes'
    """
    relative_url_usf = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/USF.xlsx"
    relative_url_equipes_cnes = '/Shared Documents/SESAU/BI_Indicadores_Estrategicos/EQUIPES_CNES.xlsx'

    dataframes_usf = get_file_as_dataframes(relative_url_usf)
    dataframes_equipes_cnes = get_file_as_dataframes(relative_url_equipes_cnes)

    # Debug statements to check loaded DataFrames
    print("DataFrames loaded from USF.xlsx:", dataframes_usf.keys())
    print("DataFrames loaded from EQUIPES_CNES.xlsx:", dataframes_equipes_cnes.keys())

    # Definir colunas a serem mantidas
    usf_columns = ['NOME DO SERVIDOR(A)','VINCULO PCR']  
    equipes_cnes_columns = ['SEQ_EQUIPE', 'NM_REFERENCIA']  

    # Filtrar colunas desejadas antes de processar
    if 'USF' in dataframes_usf:
        df_servidores = dataframes_usf['USF'][usf_columns]
    else:
        df_servidores = None

    if 'EQUIPES_CNES' in dataframes_equipes_cnes:
        df_equipes = dataframes_equipes_cnes['EQUIPES_CNES'][equipes_cnes_columns]
    else:
        df_equipes = None

    # Processar os DataFrames para as planilhas específicas
    df_servidores = remove_espacos_e_acentos(df_servidores)
    df_equipes = remove_espacos_e_acentos(df_equipes)

    # Debug statements to check processed DataFrames
    print("Processed DataFrame for servidores:", df_servidores.head() if df_servidores is not None else "None")
    print("Processed DataFrame for equipes:", df_equipes.head() if df_equipes is not None else "None")

    return {
        'servidores': df_servidores,
        'equipes': df_equipes
    }