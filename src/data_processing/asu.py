import os
import sys
import pandas as pd

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

def read_asu():

    #asu_monitora
    url_asu_monitora = '/Shared Documents/SESAU/NGI/asu/asu_monitora.xlsx'
    df_asu_monitora = get_file_as_dataframes(url_asu_monitora)

    asu_monitora_columns = ['mes', 'ine', 'resposta', 'tipo_resposta', 'pergunta_id','total_respostas']
    df_asu_monitora = df_asu_monitora['faixas_asu']    
    df_asu_monitora = df_asu_monitora[asu_monitora_columns]
    df_asu_monitora = remove_espacos_e_acentos(df_asu_monitora)
    df_asu_monitora = add_pk(df_asu_monitora, 'asu_monitora')

    # asu_classificacao
    url_asu_classificacao = '/Shared Documents/SESAU/NGI/asu/tooltip_classificacao_asu.xlsx'
    df_asu_classificacao = get_file_as_dataframes(url_asu_classificacao)

    asu_classificacao_columns = ['Atributo', 'valor', 'numero']
    df_asu_classificacao = df_asu_classificacao['Planilha1']
    df_asu_classificacao = df_asu_classificacao[asu_classificacao_columns]
    df_asu_classificacao = remove_espacos_e_acentos(df_asu_classificacao)
    df_asu_classificacao = add_pk(df_asu_classificacao, 'asu_classificacao')

    # equipes_asu
    url_equipes_asu = '/Shared Documents/SESAU/NGI/asu/asu_provisório.xlsx'
    df_equipes_asu = get_file_as_dataframes(url_equipes_asu)
    df_equipes_asu = df_equipes_asu['equipes_asu']
    df_equipes_asu = remove_espacos_e_acentos(df_equipes_asu)
    df_equipes_asu = add_pk(df_equipes_asu, 'equipes_asu')

    # unidades_equipes_asu
    url_unidades_equipes_asu = '/Shared Documents/SESAU/NGI/asu/asu_provisório.xlsx'

    try:
        df_unidades_equipes_asu = get_file_as_dataframes(url_unidades_equipes_asu)
        df_unidades_equipes_asu = df_unidades_equipes_asu['unidades_equipes_asu']
        df_unidades_equipes_asu = remove_espacos_e_acentos(df_unidades_equipes_asu)
        df_unidades_equipes_asu = add_pk(df_unidades_equipes_asu, 'unidades_equipes_asu')
    except KeyError as e:
        print(f"Erro: A aba 'unidades_equipes_asu' não foi encontrada no arquivo Excel: {e}")
    except Exception as e:
        print(f"Ocorreu um erro ao ler a aba 'unidades_equipes_asu': {e}")

    return {
        'asu_monitora': df_asu_monitora,
        'asu_classificacao': df_asu_classificacao,
        'equipes_asu': df_equipes_asu,
        'unidades_equipes_asu': df_unidades_equipes_asu
    }
