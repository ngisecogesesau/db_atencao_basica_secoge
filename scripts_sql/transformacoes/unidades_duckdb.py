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

    conn.execute("""
        INSERT INTO unidades.unidades (cnes_padrao, codigo_unidade, nome, distrito, unidade, x_long, y_lat)
        SELECT cnes_padrao, codigo_unidade, nome, distrito, unidade, x_long, y_lat FROM df_unidades
    """)

def create_tipo_unidade_table(conn, dfs):
    """
    Create 'tipo_unidade' table in DuckDB using the processed DataFrame.
    """
    df_tipo_unidade = dfs['tipo_unidade']

    conn.execute("CREATE SCHEMA IF NOT EXISTS unidades")

    conn.execute("""
        CREATE TABLE IF NOT EXISTS unidades.tipo_unidade (
            tipo_unidade VARCHAR,
            descricao VARCHAR
        )
    """)

    conn.execute("""
        INSERT INTO unidades.tipo_unidade (tipo_unidade, descricao)
        SELECT tipo_unidade, descricao FROM df_tipo_unidade
    """)

def create_horarios_table(conn, dfs):
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
    main()

