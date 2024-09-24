import pandas as pd

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_atendimentos():
    """
    Ler e processar dados para a tabela de 'atendimentos'.

    :return: Um DataFrame processado para 'atendimentos'
    """
    relative_url_atendimentos = "/Shared Documents/SESAU/NGI/atendimentos/Atendimentos.xlsx"

    dataframes_atendimentos = get_file_as_dataframes(relative_url_atendimentos)

    df_atendimentos = dataframes_atendimentos['Sheet1']

    required_columns = ['dia', 'nu_ine', 'nu_cnes', 'no_cbo', 'no_profissional','ds_turno',
                        'qt_atendimentos']
    
    df_atendimentos = df_atendimentos[required_columns]
    df_atendimentos = remove_espacos_e_acentos(df_atendimentos)
    df_atendimentos = add_pk(df_atendimentos, 'atendimentos')

    return {
        'atendimentos': df_atendimentos,
    }
