import pandas as pd
import os
import sys

# Codigo aqui por enquanto caso seja util pra debugar; remover depois
current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_googlesheet_df import get_file_as_dataframes_google
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk


def read_gratificacoes():
    url_gratificacoes_unidades = "1Kp5aaAAr9o9qXqy-JRPglKwK3TJM-O3PiS6-zzI6iBA"
    df_gratificacoes_unidades = get_file_as_dataframes_google(url_gratificacoes_unidades)
    df_gratificacoes_unidades = df_gratificacoes_unidades['RAW-DATA']
    df_gratificacoes_unidades = remove_espacos_e_acentos(df_gratificacoes_unidades)
    df_gratificacoes_unidades = add_pk(df_gratificacoes_unidades, 'gratificacoes_unidades')

    return {
        'gratificacoes_unidades': df_gratificacoes_unidades
    }

