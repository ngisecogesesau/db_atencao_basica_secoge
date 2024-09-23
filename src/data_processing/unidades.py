import pandas as pd
import logging
import re

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

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

def process_login_senha_unidades_data(dataframes):
    df_login_senha_unidades = remove_espacos_e_acentos(dataframes['Login_senha_unidades'])
    return df_login_senha_unidades

def read_unidades():
    """
    Read, process, and model data from USF and Unidades files.

    :return: A dictionary with processed and modeled DataFrames for all sheets
    """
    url_usf = "/Shared Documents/SESAU/NGI/unidades/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
    url_unidades = "/Shared Documents/SESAU/NGI/unidades/Unidades.xlsx"
    url_login_senha_unidades ="/Shared Documents/SESAU/NGI/senhas/Login_senha_unidades.xlsx"

    dataframes_usf = get_file_as_dataframes(url_usf)
    dataframes_unidades = get_file_as_dataframes(url_unidades)
    dataframes_login_senha_unidades = get_file_as_dataframes(url_login_senha_unidades)
    
    usf_data = process_usf_data(dataframes_usf)
    unidades_data = process_unidades_data(dataframes_unidades)
    login_senha_unidades_data = process_login_senha_unidades_data(dataframes_login_senha_unidades)

    data = {**usf_data, **unidades_data, 'login_senha_unidades': login_senha_unidades_data}

    df_unidades = create_df_unidades(data)
    df_tipo_unidade = create_df_tipo_unidade(data)
    df_horarios = create_df_horarios(data)
    df_distritos = create_df_distritos(data)
    df_login_senha_ds = create_login_senha_ds(data)
    df_login_senha_unidades = create_login_senha_unidades(data)


    df_unidades = remove_espacos_e_acentos(df_unidades)
    df_tipo_unidade = remove_espacos_e_acentos(df_tipo_unidade)
    df_horarios = remove_espacos_e_acentos(df_horarios)
    df_distritos = remove_espacos_e_acentos(df_distritos)
    df_login_senha_ds = remove_espacos_e_acentos(df_login_senha_ds)
    df_login_senha_unidades = remove_espacos_e_acentos(df_login_senha_unidades)


    df_unidades = add_pk(df_unidades, 'unidades')
    df_tipo_unidade = add_pk(df_tipo_unidade, 'tipo_unidade')
    df_horarios = add_pk(df_horarios, 'horarios')
    df_distritos = add_pk(df_distritos, 'distritos')
    df_login_senha_ds = add_pk(df_login_senha_ds, 'login_senha_ds')
    df_login_senha_unidades = add_pk(df_login_senha_unidades, 'login_senha_unidades')



    return {
        'unidades': df_unidades,
        'tipo_unidade': df_tipo_unidade,
        'horarios': df_horarios,
        'distritos': df_distritos,
        'login_senha_ds': df_login_senha_ds,
        'login_senha_unidades': df_login_senha_unidades
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
    
    required_columns = ['unidade']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")
    
    unique_units = df_planilha1[['unidade']].drop_duplicates()

    unit_mapping = {
        'USF': 'Unidade de Saúde da Família',
        'USF +': 'Unidade de Saúde da Família mais',
        'UBT': 'Unidade Básica Tradicional',
        'CS': 'Centro de Saúde'
    }

    tipo_unidade_data = []

    for _, row in unique_units.iterrows():
        descricao = unit_mapping.get(row['unidade'], 'Descrição desconhecida')
        tipo_unidade_data.append({'tipo_unidade': row['unidade'], 'descricao': descricao})

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

def create_df_distritos(data):
    def int_to_roman(input):
        if not isinstance(input, int):
            raise TypeError("expected integer, got %s" % type(input))
        if not 0 < input < 4000:
            raise ValueError("Argument must be between 1 and 3999")
        int_to_roman_dict = [
            (1000, "M"), (900, "CM"), (500, "D"), (400, "CD"),
            (100, "C"), (90, "XC"), (50, "L"), (40, "XL"),
            (10, "X"), (9, "IX"), (5, "V"), (4, "IV"),
            (1, "I")
        ]
        result = []
        for (integer, numeral) in int_to_roman_dict:
            count = input // integer
            result.append(numeral * count)
            input -= integer * count
        return "".join(result)

    numeros_distritos = list(range(1, 9))

    distritos = {
        'nome_distrito': [f"Distrito {int_to_roman(i)}" for i in numeros_distritos],
        'sigla_distrito': [f"{int_to_roman(i)}" for i in numeros_distritos],
        'distrito_num_inteiro': numeros_distritos  
    }

    df_distritos = pd.DataFrame(distritos)

    logging.info("Tabela df_distritos criada manualmente: %s", df_distritos)

    return df_distritos

def create_login_senha_ds(data):
    login_senha_ds = {
        'login': ['SESAU', 'SESAU', 'SESAU', 'SESAU', 'SESAU', 'SESAU', 'SESAU', 'SESAU',
                  'DS I', 'DS II', 'DS III', 'DS IV', 'DS V', 'DS VI', 'DS VII', 'DS VIII'],
        'senha': [3216, 3216, 3216, 3216, 3216, 3216, 3216, 3216, 3097,
                  3017, 3226, 3117, 3236, 3157, 3316, 3386],
        'ds_romano': ['I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII',
               'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII']
    }

    df_login_senha_ds = pd.DataFrame(login_senha_ds)
    return df_login_senha_ds

def create_login_senha_unidades(data):
    if 'login_senha_unidades' not in data:
        raise ValueError("'login_senha_unidades' not found in data")

    df_login_senha_unidades = data['login_senha_unidades']
    logging.info("Columns in 'login_senha_unidades': %s", df_login_senha_unidades.columns)

    required_columns = ['login_us', 'senha', 'no_us']
    missing_columns = [col for col in required_columns if col not in df_login_senha_unidades.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'login_senha_unidades': {missing_columns}")

    df_login_senha_unidades = df_login_senha_unidades[required_columns].rename(columns={
        'login_us': 'login',
        'senha': 'senha',
        'no_us': 'no_us'
    })

    return df_login_senha_unidades

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