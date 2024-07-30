import duckdb
import pandas as pd
import os
import sys
import re
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data_processing.unidades import read_unidades

def remove_decimal_zero(df, columns):
    """
    Remove trailing .0 from specified columns in the DataFrame.
    """
    for col in columns:
        df[col] = df[col].apply(lambda x: str(int(x)) if pd.notna(x) else x)
    return df

def create_unidades_table(con, data):
    """
    Create the 'unidades' table in DuckDB and return it as a DataFrame.
    """
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

    con.execute("""
        CREATE TABLE IF NOT EXISTS unidades (
            id_unidades INTEGER PRIMARY KEY,
            cnes_padrao VARCHAR NOT NULL,
            nome VARCHAR NOT NULL,
            unidade VARCHAR NOT NULL,
            cod_unidade VARCHAR NOT NULL,
            x_long DECIMAL,
            y_lat DECIMAL
        )
    """)
    
    con.execute("INSERT INTO unidades SELECT * FROM df_unidades")

    con.execute("""
        ALTER TABLE unidades ADD COLUMN fk_id_tipounidade INTEGER;  
        UPDATE unidades 
        SET fk_id_tipounidade = tipoUnidade.id_tipounidade
        FROM tipoUnidade
        WHERE unidades.cod_unidade = tipoUnidade.cod_unidade
    """)
    

    logging.info("Tabela 'unidades' criada com sucesso.")
    return df_unidades

def create_tipo_unidade_table(con, data):
    """
    Create the 'tipoUnidade' table in DuckDB and return it as a DataFrame.
    """
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

    con.execute("""
        CREATE TABLE IF NOT EXISTS tipoUnidade (
            tipo_unidade VARCHAR,
            descricao VARCHAR(255),
            cod_unidade INTEGER
        )
    """)

    con.execute("""
        CREATE SEQUENCE id_sequence_tipounidade START 1;
        ALTER TABLE tipoUnidade ADD COLUMN id_tipounidade INTEGER DEFAULT nextval('id_sequence_tipounidade');
""")

    con.execute("""
        INSERT INTO tipoUnidade (tipo_unidade, descricao, cod_unidade)
        SELECT tipo_unidade, descricao, cod_unidade
        FROM df_tipo_unidade
    """)

    df_tipo_unidade = con.execute("SELECT * FROM tipoUnidade").fetchdf()

    logging.info("Tabela 'tipoUnidade' criada com sucesso.")
    return df_tipo_unidade

def extract_hours(horario_str):
    """
    Extract start and end hours from a string with format '08h às 17h'.
    
    :param horario_str: String containing hours in format '08h às 17h'
    :return: Tuple of (start_hour, end_hour) or (None, None) if format is invalid
    """
    if pd.isna(horario_str):
        return None, None
    
    match = re.match(r'(\d{1,2})h\s+às\s+(\d{1,2})h', horario_str)
    if match:
        start_hour = match.group(1)
        end_hour = match.group(2)
        return start_hour + 'h', end_hour + 'h'
    return None, None

def create_horarios_table(con, data):
    """
    Create the 'horarios' table in DuckDB and return it as a DataFrame.
    """
    if 'usf_geral_2' not in data:
        raise ValueError("'usf_geral_2' not found in data")
    
    df_usf_geral_2 = data['usf_geral_2']
    
    logging.info("Columns in 'usf_geral_2': %s", df_usf_geral_2.columns)

    required_columns = ['turno', 'horario']
    missing_columns = [col for col in required_columns if col not in df_usf_geral_2.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'usf_geral_2': {missing_columns}")

    df_usf_geral_2[['horario_inicio', 'horario_fim']] = df_usf_geral_2['horario'].apply(lambda x: pd.Series(extract_hours(x)))

    df_horarios = df_usf_geral_2[required_columns + ['horario_inicio', 'horario_fim']]
    
    df_horarios = df_horarios.rename(columns={
        'turno': 'turno',
        'horario': 'horario',
        'horario_inicio': 'horario_inicio',
        'horario_fim': 'horario_fim'
    })

    con.execute("""
        CREATE TABLE IF NOT EXISTS horarios (
            id_horario INTEGER PRIMARY KEY,
            turno VARCHAR NOT NULL,
            horario VARCHAR NOT NULL,
            horario_inicio VARCHAR,
            horario_fim VARCHAR,
            id_unidades INTEGER,
            FOREIGN KEY (id_unidades) REFERENCES unidades(id_unidades)
        )
    """)

    df_horarios = df_horarios.reset_index()
    df_horarios.rename(columns={'index': 'id_horario'}, inplace=True)

    con.execute("""
        CREATE OR REPLACE TABLE horarios_temp AS 
        SELECT * FROM df_horarios
    """)

    con.execute("""
        INSERT INTO horarios (id_horario, turno, horario, horario_inicio, horario_fim, id_unidades)
        SELECT id_horario, turno, horario, horario_inicio, horario_fim, NULL AS id_unidades
        FROM horarios_temp
    """)

    df_horarios_result = con.execute("SELECT * FROM horarios").fetchdf()
    logging.info("Tabela 'horarios' criada com sucesso.")
    return df_horarios_result

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_unidades()
    df_tipo_unidade = create_tipo_unidade_table(con, data)
    df_horarios = create_horarios_table(con, data)
    df_unidades = create_unidades_table(con, data)
    logging.info("Table 'unidades' created successfully.")
    logging.info("Table 'tipoUnidade' created successfully.")
    logging.info("Table 'horarios' created successfully.")
    logging.info("DataFrame 'horarios':\n%s", df_horarios)
