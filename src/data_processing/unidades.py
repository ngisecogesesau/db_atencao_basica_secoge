import os
import sys
import pandas as pd
import logging
import re

current_dir = os.path.dirname(os.path.abspath(__file__))
root_dir = os.path.dirname(os.path.dirname(current_dir))
sys.path.append(root_dir)

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

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
    df_distritos = create_df_distritos(data)

    df_unidades = remove_espacos_e_acentos(df_unidades)
    df_tipo_unidade = remove_espacos_e_acentos(df_tipo_unidade)
    df_horarios = remove_espacos_e_acentos(df_horarios)
    df_info_unidades = remove_espacos_e_acentos(df_info_unidades)
    df_distritos = remove_espacos_e_acentos(df_distritos)

    df_unidades = add_pk(df_unidades, 'unidades')
    df_tipo_unidade = add_pk(df_tipo_unidade, 'tipo_unidade')
    df_horarios = add_pk(df_horarios, 'horarios')
    df_info_unidades = add_pk(df_info_unidades, 'info_unidades')
    df_distritos = add_pk(df_distritos, 'distritos')

    return {
        'unidades': df_unidades,
        'tipo_unidade': df_tipo_unidade,
        'horarios': df_horarios,
        'info_unidades': df_info_unidades,
        'distritos': df_distritos
    }

def create_df_unidades(data):
    if 'planilha1' not in data:
        raise ValueError("'planilha1' not found in data")
    
    df_planilha1 = data['planilha1']
    logging.info("Columns in 'planilha1': %s", df_planilha1.columns)
    
    required_columns = ['cnes_padrao', 'codigo_unidade', 'nome', 'distrito', 'unidade', 'x_long', 'y_lat','horario']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")

    df_planilha1 = remove_decimal_zero(df_planilha1, ['cnes_padrao', 'codigo_unidade'])

    df_unidades = df_planilha1[required_columns].rename(columns={
        'cnes_padrao': 'cnes',
        'codigo_unidade': 'cod_unidade',
        'nome': 'nome',
        'distrito': 'distrito',
        'unidade': 'tipo_unidade',
        'horario':'horario',
        'x_long': 'x_long',
        'y_lat': 'y_lat'
    })

    return df_unidades

def create_df_tipo_unidade(data):
    if 'planilha1' not in data:
        raise ValueError("'planilha1' not found in data")
    
    df_planilha1 = data['planilha1']
    logging.info("Columns in 'planilha1': %s", df_planilha1.columns)
    
    required_columns = ['cnes_padrao', 'unidade']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")
    
    unique_units = df_planilha1[['cnes_padrao', 'unidade']].drop_duplicates()

    unit_mapping = {
        'USF': 'Unidade de Saúde da Família',
        'USF +': 'Unidade de Saúde da Família mais',
        'UBT': 'Unidade Básica Tradicional',
        'CS': 'Centro de Saúde'
    }

    tipo_unidade_data = []

    for _, row in unique_units.iterrows():
        descricao = unit_mapping.get(row['unidade'], 'Descrição desconhecida')
        tipo_unidade_data.append({'cnes': row['cnes_padrao'], 'tipo_unidade': row['unidade'], 'descricao': descricao})

    df_tipo_unidade = pd.DataFrame(tipo_unidade_data)
    
    return df_tipo_unidade

def create_df_horarios(data):
    if 'planilha1' not in data:
        raise ValueError("'planilha1' not found in data")
    
    df_planilha1 = data['planilha1']
    
    required_columns = ['cnes_padrao', 'horario']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")

    def extract_hours(horario):
        if pd.isna(horario):
            return [None, None]

        match = re.match(r'(\d{2})h\s*às\s*(\d{2})h', horario)
        if match:
            horario_inicio, horario_fim = match.groups()
            return [horario_inicio, horario_fim]
        else:
            return [None, None]

    def determine_turno(horario_inicio, horario_fim):
        if not horario_inicio or not horario_fim:
            return 'Desconhecido'
        try:
            inicio = int(horario_inicio.replace('h', ''))
            fim = int(horario_fim.replace('h', ''))
        except ValueError:
            return 'Desconhecido'

        if inicio < 12 and fim <= 12:
            return 'Diurno'
        elif inicio < 12 and fim > 12:
            return 'Integral'
        elif inicio >= 18:
            return 'Noturno'
        elif inicio < 12 and fim > 18:
            return 'Integral'
        else:
            return 'Desconhecido'

    df_planilha1[['horario_inicio', 'horario_fim']] = df_planilha1['horario'].apply(lambda x: pd.Series(extract_hours(x)))
    df_planilha1['turno'] = df_planilha1.apply(lambda row: determine_turno(row['horario_inicio'], row['horario_fim']), axis=1)

    df_horarios = df_planilha1[['cnes_padrao', 'horario', 'horario_inicio', 'horario_fim', 'turno']]
    df_horarios = df_horarios.rename(columns={
        'cnes_padrao': 'cnes',
        'horario': 'horario',
        'horario_inicio': 'horario_inicio',
        'horario_fim': 'horario_fim',
        'turno': 'turno'
    }).drop_duplicates()

    return df_horarios

def create_df_info_unidades(data):
    if 'usf_plus' not in data:
        raise ValueError("'usf_plus' not found in data")
    
    df_usf_plus = data['usf_plus']
    
    required_columns = [
        'cnes', 'nome', 'perfil', 'distrito', 'complexidade',
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
        'cnes': 'cnes',
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

def create_df_distritos(data):
    if 'planilha1' not in data:
        raise ValueError("'planilha1' not found in data")
    
    df_planilha1 = data['planilha1']
    logging.info("Columns in 'planilha1': %s", df_planilha1.columns)
    
    required_columns = ['cnes_padrao', 'distrito']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")

    df_planilha1 = remove_decimal_zero(df_planilha1, ['cnes_padrao'])

    df_distritos = df_planilha1[['cnes_padrao', 'distrito']].drop_duplicates().rename(columns={'distrito': 'sigla_distrito','cnes_padrao':'cnes'})

    df_distritos['nome_distrito'] = df_distritos['sigla_distrito'].apply(lambda x: f"Distrito {x}")

    df_distritos = df_distritos[['nome_distrito', 'sigla_distrito', 'cnes']]

    return df_distritos

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