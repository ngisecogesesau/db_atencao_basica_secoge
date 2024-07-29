import os
import sys

# Ensure the root directory is in the sys.path
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos

def read_unidades():
    """
    Read and process data from USF and Unidades files.

    :return: A dictionary with processed DataFrames for all sheets
    """
    url_usf = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
    url_unidades = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/Unidades.xlsx"

    dataframes_usf = get_file_as_dataframes(url_usf)
    dataframes_unidades = get_file_as_dataframes(url_unidades)

    # Process the DataFrames for the specific sheets
    df_usf_geral = remove_espacos_e_acentos(dataframes_usf['USF GERAL'])
    df_usf_plus = remove_espacos_e_acentos(dataframes_usf['USF+'])
    df_usf_geral_2 = remove_espacos_e_acentos(dataframes_usf['USF GERAL 2'])
    
    df_planilha1 = remove_espacos_e_acentos(dataframes_unidades['Planilha1'])
    df_planilha2 = remove_espacos_e_acentos(dataframes_unidades['Planilha2'])
    df_planilha3 = remove_espacos_e_acentos(dataframes_unidades['Planilha3'])

    return {
        'usf_geral': df_usf_geral,
        'usf_plus': df_usf_plus,
        'usf_geral_2': df_usf_geral_2,
        'planilha1': df_planilha1,
        'planilha2': df_planilha2,
        'planilha3': df_planilha3
    }

if __name__ == '__main__':
    data = read_unidades()
    for key, df in data.items():
        print(f"DataFrame {key}:")
        print(df.head())
