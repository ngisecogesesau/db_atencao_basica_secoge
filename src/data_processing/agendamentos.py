import os
import sys
import pandas as pd

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_agendamentos():
    """
    Ler e processar dados para a tabela de 'agendamentos' a partir da aba 'Planilha1'.

    :return: Um DataFrame processado para 'agendamentos'
    """
    relative_url_agendamentos = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/agendamentos.xlsx"

    dataframes_agendamentos = get_file_as_dataframes(relative_url_agendamentos)

    df_agendamentos = dataframes_agendamentos.get('Planilha1')

    if df_agendamentos is not None:
        
        df_agendamentos = remove_espacos_e_acentos(df_agendamentos)
        df_agendamentos = add_pk(df_agendamentos, 'agendamentos')
    else:
        print("Aba 'Planilha1' não encontrada no arquivo Excel.")
        df_agendamentos = pd.DataFrame()

    return {
        'agendamentos': df_agendamentos,
    }

if __name__ == "__main__":
    df = read_agendamentos()
    print(df.head())


# import os
# import pandas as pd
# import sys

# current_dir = os.path.dirname(os.path.abspath(__file__))
# root_dir = os.path.dirname(os.path.dirname(current_dir))
# sys.path.append(root_dir)

# from src.utils.excel_operations import remove_espacos_e_acentos
# from src.utils.add_primary_key import add_pk

# def read_agendamentos():
#     """
#     Ler e processar dados para a tabela de 'agendamentos' a partir da aba 'Planilha1'.
    
#     :return: Um DataFrame processado para 'agendamentos'
#     """
#     # Caminho do arquivo agendamentos.xlsx no mesmo diretório que o script
#     current_dir = os.path.dirname(os.path.abspath(__file__))
#     file_path = os.path.join(current_dir, 'agendamentos.xlsx')

#     # Ler o arquivo Excel
#     df_agendamentos = pd.read_excel(file_path, sheet_name='Planilha1')

#     if df_agendamentos is not None:
#         # Processar os dados removendo espaços e acentos
#         df_agendamentos = remove_espacos_e_acentos(df_agendamentos)

#         # Adicionar a chave primária ao DataFrame
#         df_agendamentos = add_pk(df_agendamentos, 'agendamentos')
#     else:
#         print("A aba 'Planilha1' não foi encontrada no arquivo Excel.")
#         df_agendamentos = pd.DataFrame()

#     return df_agendamentos

# # Se for executar este script diretamente, você pode adicionar uma função principal para testes
# if __name__ == "__main__":
#     df = read_agendamentos()
#     print(df.head())
