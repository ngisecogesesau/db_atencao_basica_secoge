import pandas as pd
import os
import sys

# Codigo aqui por enquanto caso seja util pra debugar; remover depois
# current_dir = os.path.dirname(os.path.abspath(__file__))
# root_dir = os.path.dirname(os.path.dirname(current_dir))
## print(root_dir)
# sys.path.append(root_dir)

from src.utils.extract_googlesheet_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk


def read_gratificacoes():
    url_gratificacoes = "1Kp5aaAAr9o9qXqy-JRPglKwK3TJM-O3PiS6-zzI6iBA"
    df_gratificacoes = get_file_as_dataframes(url_gratificacoes)
    # ... resto do codigo ...
