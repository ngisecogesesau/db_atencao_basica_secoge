import pandas as pd
import os
import sys
import re

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.add_primary_key import add_pk
from src.utils.excel_operations import remove_espacos_e_acentos

def read_sevs_criancas_risco():
   
    url_criancas_risco = '/Shared Documents/SESAU/NGI/sevs_crianca_de_risco/VIGILANCIA DA CRIANÇA DE RISCO.xlsx'
    df_criancas_risco = get_file_as_dataframes(url_criancas_risco, sheet_name="Planilha1")
    df_criancas_risco = df_criancas_risco['Planilha1']

    df_criancas_risco.iloc[:, 0] = df_criancas_risco.iloc[:, 0].astype(str).str.strip()

    title_patterns = [
        r'^Nº de crianças de risco .*',
        r'^Nº\s+de atendimentos das crianças de risco .*',
        r'^% de crianças de risco .*'
    ]

    def is_title(row_value):
        if pd.isnull(row_value):
            return False
        for pattern in title_patterns:
            if re.match(pattern, row_value.strip()):
                return True
        return False

    def clean_table(df, is_first_table=False):
        """
        Function to clean and organize the table by removing unwanted rows and columns.
        """
        df = df.dropna(how='all', axis=1).dropna(how='all', axis=0)
        
        if df.empty:
            print("Warning: DataFrame is empty after dropping NaNs.")
            return df  
        
        df.reset_index(drop=True, inplace=True)
        
        if is_first_table:
            header_row_index = 0
        else:
            df.iloc[:, 0] = df.iloc[:, 0].astype(str).str.strip().str.lower()
            header_row_index_list = df[df.iloc[:, 0] == 'ds de residência'].index
            if not header_row_index_list.empty:
                header_row_index = header_row_index_list[0]
            else:
                print("Header row not found. Unable to set DataFrame columns.")
                return df  
       
        df.columns = df.iloc[header_row_index]
        df = df.iloc[header_row_index+1:]
        df.reset_index(drop=True, inplace=True)
        
        return df

    title_rows = df_criancas_risco[df_criancas_risco.iloc[:, 0].apply(lambda x: is_title(str(x)))]
    indices = title_rows.index.tolist()

    if indices and indices[0] != 0:
        indices = [0] + indices

    if len(indices) < 6:
        print("Error: Expected at least 6 table titles in the data.")
        print("Found indices:", indices)
        print("Please check the data and adjust the code accordingly.")
        return

    df_criancas_elegiveis = df_criancas_risco.iloc[indices[0]:indices[1]].copy()
    df_criancas_distribuidas = df_criancas_risco.iloc[indices[1]+1:indices[2]].copy()
    df_criancas_acompanhadas = df_criancas_risco.iloc[indices[2]+1:indices[3]].copy()
    df_atendimentos = df_criancas_risco.iloc[indices[3]+1:indices[4]].copy()
    df_percentual_distribuidas = df_criancas_risco.iloc[indices[4]+1:indices[5]].copy()
    df_percentual_acompanhadas = df_criancas_risco.iloc[indices[5]+1:].copy()
    

    df_criancas_elegiveis = clean_table(df_criancas_elegiveis, is_first_table=True)
    df_criancas_elegiveis = remove_espacos_e_acentos(df_criancas_elegiveis)
    df_criancas_elegiveis = add_pk(df_criancas_elegiveis, 'criancas_risco_elegiveis')

    df_criancas_distribuidas = clean_table(df_criancas_distribuidas)
    df_criancas_distribuidas = remove_espacos_e_acentos(df_criancas_distribuidas)
    df_criancas_distribuidas = add_pk(df_criancas_distribuidas, 'criancas_risco_distribuidas')

    df_criancas_acompanhadas = clean_table(df_criancas_acompanhadas)
    df_atendimentos = clean_table(df_atendimentos)
    df_percentual_distribuidas = clean_table(df_percentual_distribuidas)
    df_percentual_acompanhadas = clean_table(df_percentual_acompanhadas)

    return {
        'criancas_risco_elegiveis': df_criancas_elegiveis,
        'criancas_risco_distribuidas': df_criancas_distribuidas,
        'criancas_acompanhadas': df_criancas_acompanhadas,
        'criancas_atendimentos': df_atendimentos,
        'criancas_percentual_distribuidas': df_percentual_distribuidas,
        'criancas_percentual_acompanhadas': df_percentual_acompanhadas
    }