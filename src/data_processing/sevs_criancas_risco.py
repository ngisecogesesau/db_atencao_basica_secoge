import pandas as pd
import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.add_primary_key import add_pk
from src.utils.excel_operations import remove_espacos_e_acentos

def read_criancas_de_risco():
   
    url_criancas_risco = '/Shared Documents/SESAU/NGI/sevs_asace/VIGILANCIA DA CRIANÇA DE RISCO.xlsx'
    df_criancas_risco = get_file_as_dataframes(url_criancas_risco, sheet_name="Planilha1")
    df_criancas_risco = df_criancas_risco['Planilha1']


    
#     # Identificar os índices onde as tabelas começam
#     indices = df_criancas_risco[df_criancas_risco.iloc[:, 0].str.contains('Nº de crianças de risco', na=False)].index.tolist()
    
#     # Separar as tabelas usando os índices identificados
#     df_criancas_elegiveis = df_criancas_risco.iloc[indices[0]+1:indices[1]].copy()
#     df_criancas_distribuidas = df_criancas_risco.iloc[indices[1]+1:indices[2]].copy()
#     df_criancas_acompanhadas = df_criancas_risco.iloc[indices[2]+1:indices[3]].copy()
#     df_atendimentos = df_criancas_risco.iloc[indices[3]+1:indices[4]].copy()
#     df_percentual_distribuidas = df_criancas_risco.iloc[indices[4]+1:indices[5]].copy()
#     df_percentual_acompanhadas = df_criancas_risco.iloc[indices[5]+1:].copy()
    
#     # Limpeza e organização das tabelas
#     df_criancas_elegiveis = clean_table(df_criancas_elegiveis)
#     df_criancas_distribuidas = clean_table(df_criancas_distribuidas)
#     df_criancas_acompanhadas = clean_table(df_criancas_acompanhadas)
#     df_atendimentos = clean_table(df_atendimentos)
#     df_percentual_distribuidas = clean_table(df_percentual_distribuidas)
#     df_percentual_acompanhadas = clean_table(df_percentual_acompanhadas)

#     print(df_criancas_elegiveis)
#     print(df_percentual_acompanhadas)
    
#     # Retornar as tabelas como dicionário de DataFrames
#     return {
#         'criancas_elegiveis': df_criancas_elegiveis,
#         'criancas_distribuidas': df_criancas_distribuidas,
#         'criancas_acompanhadas': df_criancas_acompanhadas,
#         'atendimentos': df_atendimentos,
#         'percentual_distribuidas': df_percentual_distribuidas,
#         'percentual_acompanhadas': df_percentual_acompanhadas
#     }

# def clean_table(df):
#     """
#     Função para limpar e organizar a tabela removendo linhas e colunas indesejadas.
#     """
#     # Remove colunas e linhas totalmente nulas e ajusta cabeçalhos
#     df = df.dropna(how='all', axis=1).dropna(how='all', axis=0)
#     df.columns = df.iloc[0]
#     df = df[1:]
#     df.reset_index(drop=True, inplace=True)
#     return df

read_criancas_de_risco()