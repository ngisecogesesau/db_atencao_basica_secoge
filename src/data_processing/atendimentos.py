import os
import sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_atendimentos():
    """
    Ler e processar dados para a tabela de 'atendimentos'.

    :return: Um DataFrame processado para 'atendimentos'
    """
    relative_url_atendimentos = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/Atendimentos.xlsx"

    dataframes_atendimentos = get_file_as_dataframes(relative_url_atendimentos)

    df_atendimentos = dataframes_atendimentos.get('Sheet1')

    if df_atendimentos is not None:
        
        df_atendimentos = remove_espacos_e_acentos(df_atendimentos)
        df_atendimentos = add_pk(df_atendimentos, 'atendimentos')
    else:
        print("Aba 'Sheet1' não encontrada no arquivo Excel.")
        df_atendimentos = pd.DataFrame()

    return {
        'atendimentos': df_atendimentos,
    }

if __name__ == "__main__":
    df = read_atendimentos()
    print(df.head())