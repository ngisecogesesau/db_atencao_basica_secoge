import duckdb
import pandas as pd
import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data_processing.unidades import read_unidades

def create_unidades_table(conn, dfs):
    """
    Create 'unidades' table in DuckDB using the processed DataFrame.
    """
    df_unidades = dfs['unidades']

    conn.execute("CREATE SCHEMA IF NOT EXISTS unidades")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS unidades.unidades (
            cnes_padrao VARCHAR,
            codigo_unidade VARCHAR,
            nome VARCHAR,
            distrito VARCHAR,
            unidade VARCHAR,
            x_long DOUBLE,
            y_lat DOUBLE
        )
    """)
    
    con.execute("INSERT INTO unidades SELECT * FROM df_unidades")
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
        CREATE TABLE IF NOT EXISTS tipoUnidade_temp (
            tipo_unidade VARCHAR,
            descricao VARCHAR(255)
        )
    """)

    con.execute("""
        INSERT INTO tipoUnidade_temp (tipo_unidade, descricao)
        SELECT tipo_unidade, descricao
        FROM df_tipo_unidade
    """)

    con.execute("""
        CREATE TABLE IF NOT EXISTS tipoUnidade AS
        SELECT ROW_NUMBER() OVER (ORDER BY tipo_unidade) AS id_tipo_unidade,
               tipo_unidade,
               descricao
        FROM tipoUnidade_temp
    """)

    df_tipo_unidade_result = con.execute("SELECT * FROM tipoUnidade").fetchdf()

    logging.info("Tabela 'tipoUnidade' criada com sucesso.")
    return df_tipo_unidade_result

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
    Create 'horarios' table in DuckDB using the processed DataFrame.
    """
    df_horarios = dfs['horarios']

    conn.execute("CREATE SCHEMA IF NOT EXISTS unidades")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS unidades.horarios (
            turno VARCHAR,
            horario VARCHAR,
            horario_inicio VARCHAR,
            horario_fim VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO unidades.horarios (turno, horario, horario_inicio, horario_fim)
        SELECT turno, horario, horario_inicio, horario_fim FROM df_horarios
    """)

def create_info_unidades_table(conn, dfs):
    """
    Create 'info_unidades' table in DuckDB using the processed DataFrame.
    """
    df_info_unidades = dfs['info_unidades']

    conn.execute("CREATE SCHEMA IF NOT EXISTS unidades")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS unidades.info_unidades (
            cnes_padrao VARCHAR,
            n_esf VARCHAR,
            turno_da_esf VARCHAR,
            horario_da_esf VARCHAR,
            medico_da_esf VARCHAR,
            enfermeiro_esf VARCHAR,
            tecnico_esf VARCHAR,
            acs VARCHAR,
            n_esb VARCHAR,
            turno_esb VARCHAR,
            horario_esb VARCHAR,
            cir_dentista VARCHAR,
            asb VARCHAR,
            recepcionista VARCHAR,
            turno_do_recepcionista VARCHAR,
            horario_do_recepcionista VARCHAR,
            regulacao VARCHAR,
            turno_do_prof_regulacao VARCHAR,
            horario_do_prof_regulacao VARCHAR,
            farmacia VARCHAR,
            turno_do_prof_farmacia VARCHAR,
            horario_do_prof_farmacia VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO unidades.info_unidades (cnes_padrao, n_esf, turno_da_esf, horario_da_esf, medico_da_esf, enfermeiro_esf, tecnico_esf, acs, n_esb, turno_esb, horario_esb, cir_dentista, asb, recepcionista, turno_do_recepcionista, horario_do_recepcionista, regulacao, turno_do_prof_regulacao, horario_do_prof_regulacao, farmacia, turno_do_prof_farmacia, horario_do_prof_farmacia)
        SELECT cnes_padrao, n_esf, turno_da_esf, horario_da_esf, medico_da_esf, enfermeiro_esf, tecnico_esf, acs, n_esb, turno_esb, horario_esb, cir_dentista, asb, recepcionista, turno_do_recepcionista, horario_do_recepcionista, regulacao, turno_do_prof_regulacao, horario_do_prof_regulacao, farmacia, turno_do_prof_farmacia, horario_do_prof_farmacia FROM df_info_unidades
    """)

def main():
    """
    Main function to create tables and insert data into DuckDB.
    """
    conn = duckdb.connect('unidades.duckdb')
    
    dfs = read_unidades()

    create_unidades_table(conn, dfs)
    create_tipo_unidade_table(conn, dfs)
    create_horarios_table(conn, dfs)
    create_info_unidades_table(conn, dfs)

    conn.close()

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_unidades()
    df_unidades = create_unidades_table(con, data)
    df_tipo_unidade = create_tipo_unidade_table(con, data)
    df_horarios = create_horarios_table(con, data)
    logging.info("Table 'unidades' created successfully.")
    logging.info("Table 'tipoUnidade' created successfully.")
    logging.info("Table 'horarios' created successfully.")
    logging.info("DataFrame 'horarios':\n%s", df_horarios)
