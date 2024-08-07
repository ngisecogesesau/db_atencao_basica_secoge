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
    
    con.execute("""
        ALTER TABLE tipoUnidade ADD COLUMN fk_id_unidades INTEGER;
        
        UPDATE tipoUnidade
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE tipoUnidade.cnes = unidades.cnes;
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
    
    con.execute("""
        ALTER TABLE horarios ADD COLUMN fk_id_unidades INTEGER;
        
        UPDATE horarios
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE unidades.cnes = horarios.cnes;
                """)
    
    df_horarios = con.execute("SELECT * FROM horarios").fetchdf()
    
    logging.info("Tabela 'horarios' criada com sucesso.")
    
    return df_horarios

def create_distritos_table(con):
    """
    Create the 'distritos' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE distritos AS
        SELECT
            *
        FROM 
            distritos_temp;
    """)
    
    df_distritos = con.execute("SELECT * FROM distritos").fetchdf()
    
    logging.info("Tabela 'distritos' criada com sucesso.")
    
    return df_distritos

def update_unidades_table(con):
    con.execute("""
            ALTER TABLE unidades ADD COLUMN fk_id_tipo_unidade INTEGER;
            
            UPDATE unidades
            SET fk_id_tipo_unidade = tipoUnidade.id_tipo_unidade
            FROM tipoUnidade
            WHERE unidades.tipo_unidade = tipoUnidade.tipo_unidade;
                    
            ALTER TABLE unidades ADD COLUMN fk_id_horarios INTEGER;
            
            UPDATE unidades
            SET fk_id_horarios = horarios.id_horarios
            FROM horarios
            WHERE unidades.horario = horarios.horario;     
                    
            ALTER TABLE unidades ADD COLUMN fk_id_distritos INTEGER;
            
            UPDATE unidades
            SET fk_id_distritos = distritos.id_distritos
            FROM distritos
            WHERE unidades.distrito = sigla_distrito;   
    """)

    df_update_unidades = con.execute("SELECT * FROM unidades").fetchdf()

    logging.info("Tabela 'unidades' atualizada com sucesso.")
    
    return df_update_unidades

def create_login_senha_ds_table(con):
    """
    Create the 'login_senha_ds' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE login_senha_ds AS
        SELECT
            *
        FROM 
            login_senha_ds_temp;
    """)
    
    df_login_senha_ds = con.execute("SELECT * FROM login_senha_ds").fetchdf()
    
    logging.info("Tabela 'login_senha_ds' criada com sucesso.")

    return df_login_senha_ds

def create_login_senha_unidades_table(con):
    """
    Create the 'login_senha_unidades' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE login_senha_unidades AS
        SELECT
            *
        FROM 
            login_senha_unidades_temp;
    """)
    
    df_login_senha_ds = con.execute("SELECT * FROM login_senha_unidades").fetchdf()
    
    logging.info("Tabela 'login_senha_unidades' criada com sucesso.")

    return df_login_senha_ds

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_unidades()
    df_unidades = create_unidades_table(con, data)
    df_tipo_unidade = create_tipo_unidade_table(con, data)
    df_horarios = create_horarios_table(con, data)
    df_distritos = create_distritos_table(con,data)
    df_update_unidades = update_unidades_table(con,data)
    df_login_senha_ds = create_login_senha_ds_table(con,data)
    df_login_senha_unidades = create_login_senha_unidades_table(con,data)
    