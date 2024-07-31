import duckdb
import pandas as pd
import os
import sys
import logging
import re

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data_processing.unidades import read_unidades

def create_unidades_table(con):
    """
    Create 'unidades' table in DuckDB using the processed DataFrame.
    """
    con.execute("""
        CREATE TABLE unidades AS
        SELECT
            *
        FROM 
            unidades_temp;
    """)
    
    df_unidades = con.execute("SELECT * FROM unidades").fetchdf()

    logging.info("Tabela 'unidades' criada com sucesso.")
    
    return df_unidades

def create_tipo_unidade_table(con):
    """
    Create the 'tipoUnidade' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE tipoUnidade AS
        SELECT
            *
        FROM 
            tipo_unidade_temp;
    """)
    
    df_tipo_unidades = con.execute("SELECT * FROM tipoUnidade").fetchdf()
    
    logging.info("Tabela 'tipoUnidade' criada com sucesso.")
    
    return df_tipo_unidades

def create_horarios_table(con):
    """
    Create 'horarios' table in DuckDB using the processed DataFrame.
    """
    con.execute("""
        CREATE TABLE horarios AS
        SELECT
            *
        FROM 
            horarios_temp;
    """)
    
    df_horarios = con.execute("SELECT * FROM horarios").fetchdf()
    
    logging.info("Tabela 'horarios' criada com sucesso.")
    
    return df_horarios

def create_info_unidades_table(con):
    """
    Create 'info_unidades' table in DuckDB using the processed DataFrame.
    """
    con.execute("""
        CREATE TABLE info_unidades AS
        SELECT
            *
        FROM 
            info_unidades_temp;
    """)
    
    df_info_unidades = con.execute("SELECT * FROM info_unidades").fetchdf()
    
    logging.info("Tabela 'info_unidades' criada com sucesso.")
    
    return df_info_unidades

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
