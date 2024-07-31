import os
import sys
import pandas as pd
import logging

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

def remove_decimal_zero(df, columns):
    """
    Remove trailing .0 from specified columns in the DataFrame.
    """
    for col in columns:
        df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) else x)
    return df

def process_usf_data(dataframes):
    df_usf_geral = remove_espacos_e_acentos(dataframes['USF GERAL'])
    df_usf_plus = remove_espacos_e_acentos(dataframes['USF+'])
    df_usf_geral_2 = remove_espacos_e_acentos(dataframes['USF GERAL 2'])

    return {
        'usf_geral': df_usf_geral,
        'usf_plus': df_usf_plus,
        'usf_geral_2': df_usf_geral_2
    }

def process_unidades_data(dataframes):
    df_planilha1 = remove_espacos_e_acentos(dataframes['Planilha1'])
    df_planilha2 = remove_espacos_e_acentos(dataframes['Planilha2'])
    df_planilha3 = remove_espacos_e_acentos(dataframes['Planilha3'])
    
    df_planilha1 = remove_decimal_zero(df_planilha1, ['cnes_padrao', 'codigo_unidade'])
    
    return {
        'planilha1': df_planilha1,
        'planilha2': df_planilha2,
        'planilha3': df_planilha3
    }

def read_unidades():
    """
    Read, process, and model data from USF and Unidades files.

    :return: A dictionary with processed and modeled DataFrames for all sheets
    """
    url_usf = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
    url_unidades = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/Unidades.xlsx"

    dataframes_usf = get_file_as_dataframes(url_usf)
    dataframes_unidades = get_file_as_dataframes(url_unidades)

    usf_data = process_usf_data(dataframes_usf)
    unidades_data = process_unidades_data(dataframes_unidades)

    data = {**usf_data, **unidades_data}

    df_unidades = create_df_unidades(data)
    df_tipo_unidade = create_df_tipo_unidade(data)
    df_horarios = create_df_horarios(data)
    df_info_unidades = create_df_info_unidades(data)

    return {
        'unidades': df_unidades,
        'tipo_unidade': df_tipo_unidade,
        'horarios': df_horarios,
        'info_unidades': df_info_unidades
    }

def create_df_unidades(data):
    if 'planilha1' not in data:
        raise ValueError("'planilha1' not found in data")
    
    df_planilha1 = data['planilha1']
    logging.info("Columns in 'planilha1': %s", df_planilha1.columns)
    
    required_columns = ['cnes_padrao', 'codigo_unidade', 'nome', 'distrito', 'unidade', 'x_long', 'y_lat']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")

    df_planilha1 = remove_decimal_zero(df_planilha1, ['cnes_padrao', 'codigo_unidade'])

    df_unidades = df_planilha1[required_columns].rename(columns={
        'cnes_padrao': 'cnes_padrao',
        'codigo_unidade': 'cod_unidade',
        'nome': 'nome',
        'distrito': 'distrito',
        'unidade': 'unidade',
        'x_long': 'x_long',
        'y_lat': 'y_lat'
    })

    return df_unidades

def create_df_tipo_unidade(data):
    if 'planilha1' not in data or 'planilha2' not in data:
        raise ValueError("'planilha1' or 'planilha2' not found in data")
    
    df_planilha1 = data['planilha1']
    df_planilha2 = data['planilha2']

    unique_units_planilha1 = df_planilha1['unidade'].unique()
    unique_units_planilha2 = df_planilha2['unidade_'].unique()

    unit_mapping = {
        'USF': 'Unidade de Saúde da Família',
        'USF +': 'Unidade de Saúde da Família mais',
        'UBT': 'Unidade Básica Tradicional',
        'CS': 'Centro de Saúde'
    }

    unique_units = set(unique_units_planilha1) | set(unique_units_planilha2)
    tipo_unidade_data = []

    for unit in unique_units:
        descricao = unit_mapping.get(unit, 'Descrição desconhecida')
        tipo_unidade_data.append({'tipo_unidade': unit, 'descricao': descricao})

    df_tipo_unidade = pd.DataFrame(tipo_unidade_data)
    
    return df_tipo_unidade

def create_df_horarios(data):
    if 'usf_geral_2' not in data:
        raise ValueError("'usf_geral_2' not found in data")
    
    df_usf_geral_2 = data['usf_geral_2']
    
    required_columns = ['turno', 'horario']
    missing_columns = [col for col in required_columns if col not in df_usf_geral_2.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'usf_geral_2': {missing_columns}")

    def extract_hours(horario):
        if pd.isna(horario):
            return [None, None]
        try:
            horario_inicio, horario_fim = horario.split(' - ')
            return [horario_inicio, horario_fim]
        except ValueError:
            return [None, None]
        
    df_usf_geral_2[['horario_inicio', 'horario_fim']] = df_usf_geral_2['horario'].apply(lambda x: pd.Series(extract_hours(x)))

    df_horarios = df_usf_geral_2[required_columns + ['horario_inicio', 'horario_fim']]

    df_horarios = df_horarios.rename(columns={
        'turno': 'turno',
        'horario': 'horario',
        'horario_inicio': 'horario_inicio',
        'horario_fim': 'horario_fim'
    }).drop_duplicates()

    return df_horarios

def create_df_info_unidades(data):
    if 'usf_plus' not in data:
        raise ValueError("'usf_plus' not found in data")
    
    df_usf_plus = data['usf_plus']
    
    required_columns = [
        'cnes', 'cnes_padrao', 'nome', 'perfil', 'distrito', 'complexidade',
        'no_da_esf', 'turno_da_esf', 'horario_da_esf', 'medico_da_esf', 
        'enfermeiro_esf', 'tecnico_esf', 'acs', 'no_esb', 'turno_esb', 
        'horario_esb', 'cir._dentista', 'asb', 'recepcionista', 
        'turno_do_recepcionista', 'horario_do_recepcionista', 'regulacao', 
        'turno_do_prof._regulacao', 'horario_do_prof._regulacao', 'farmacia', 
        'turno_do_prof._farmacia', 'horario_do_prof._farmacia'
    ]
    
    missing_columns = [col for col in required_columns if col not in df_usf_plus.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'usf_plus': {missing_columns}")

    df_info_unidades = df_usf_plus[required_columns].rename(columns={
        'cnes': 'cnes_padrao',
        'no_da_esf': 'n_esf',
        'turno_da_esf': 'turno_da_esf',
        'horario_da_esf': 'horario_da_esf',
        'medico_da_esf': 'medico_da_esf',
        'enfermeiro_esf': 'enfermeiro_esf',
        'tecnico_esf': 'tecnico_esf',
        'acs': 'acs',
        'no_esb': 'n_esb',
        'turno_esb': 'turno_esb',
        'horario_esb': 'horario_esb',
        'cir._dentista': 'cir_dentista',
        'asb': 'asb',
        'recepcionista': 'recepcionista',
        'turno_do_recepcionista': 'turno_do_recepcionista',
        'horario_do_recepcionista': 'horario_do_recepcionista',
        'regulacao': 'regulacao',
        'turno_do_prof._regulacao': 'turno_do_prof_regulacao',
        'horario_do_prof._regulacao': 'horario_do_prof_regulacao',
        'farmacia': 'farmacia',
        'turno_do_prof._farmacia': 'turno_do_prof_farmacia',
        'horario_do_prof._farmacia': 'horario_do_prof_farmacia'
    })

    return df_info_unidades

def main():
    """
    Main function for testing the script.
    """
    dfs = read_unidades()
    for name, df in dfs.items():
        logging.info("DataFrame '%s':", df)
        logging.info("Columns: %s", list(df.columns))
        logging.info("Head:\n%s", df.head())

if __name__ == '__main__':
    main()