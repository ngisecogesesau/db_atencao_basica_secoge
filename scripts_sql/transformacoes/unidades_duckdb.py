import duckdb
import pandas as pd
import os
import sys

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data_processing.unidades import read_unidades

def create_unidades_table(con, data):
    """
    Create the 'unidades' table in DuckDB and return it as a DataFrame.
    """
    if 'planilha1' not in data:
        raise ValueError("'planilha1' not found in data")
    
    df_planilha1 = data['planilha1']
    print("Columns in 'planilha1':", df_planilha1.columns)
    
    required_columns = ['cnes_padrao', 'codigo_unidade', 'nome', 'distrito', 'unidade', 'x_long', 'y_lat']
    missing_columns = [col for col in required_columns if col not in df_planilha1.columns]
    if missing_columns:
        raise KeyError(f"Missing columns in 'planilha1': {missing_columns}")

    df_unidades = df_planilha1[required_columns].rename(columns={
        'cnes_padrao': 'cnes_padrao',
        'codigo_unidade': 'cod_unidade',
        'nome': 'nome',
        'unidade': 'unidade',
        'x_long': 'x_long',
        'y_lat': 'y_lat'
    })

    con.execute("""
        CREATE TABLE IF NOT EXISTS unidades_temp (
            cnes_padrao VARCHAR,
            nome VARCHAR,
            unidade VARCHAR,
            cod_unidade VARCHAR,
            x_long DECIMAL(10, 6),
            y_lat DECIMAL(10, 6)
        )
    """)

    con.execute("""
        INSERT INTO unidades_temp (cnes_padrao, nome, unidade, cod_unidade, x_long, y_lat)
        SELECT cnes_padrao, nome, unidade, cod_unidade, x_long, y_lat
        FROM df_unidades
    """)
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS unidades AS
        SELECT
            ROW_NUMBER() OVER (ORDER BY cnes_padrao) AS id_unidades,
            cnes_padrao,
            nome,
            unidade,
            cod_unidade,
            x_long,
            y_lat,
            NULL AS fk_id_tipo_unidade,
            NULL AS fk_id_horarios,
            NULL AS fk_id_info_unidades,
            NULL AS fk_id_distritos
        FROM unidades_temp
    """)

    df_result = con.execute("SELECT * FROM unidades").fetchdf()
    return df_result

def create_tipo_unidade_table(con):
    """
    Create the 'tipoUnidade' table in DuckDB and return it as a DataFrame.
    """
    
    con.execute("""
        CREATE TABLE IF NOT EXISTS tipoUnidade_temp (
            tipo_unidade VARCHAR(10),
            descricao VARCHAR(255),
            fk_id_unidades INTEGER
        )
    """)


    con.execute("""
        INSERT INTO tipoUnidade_temp (tipo_unidade, descricao, fk_id_unidades)
        VALUES ('Tipo1', 'Descrição do Tipo 1', 1),
               ('Tipo2', 'Descrição do Tipo 2', 2)
    """)


    con.execute("""
        CREATE TABLE IF NOT EXISTS tipoUnidade AS
        SELECT ROW_NUMBER() OVER (ORDER BY tipo_unidade) AS id_tipo_unidade,
               tipo_unidade,
               descricao,
               fk_id_unidades
        FROM tipoUnidade_temp
    """)

    df_tipo_unidade = con.execute("SELECT * FROM tipoUnidade").fetchdf()

    return df_tipo_unidade

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_unidades()
    df_unidades = create_unidades_table(con, data)
    df_tipo_unidade = create_tipo_unidade_table(con)
    print("Table 'unidades' created successfully.")
    print(df_unidades)
    print("Table 'tipoUnidade' created successfully.")
    print(df_tipo_unidade)
