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
        SELECT ROW_NUMBER() OVER () AS id_unidades, *
        FROM unidades_temp
    """)

    df_result = con.execute("SELECT * FROM unidades").fetchdf()
    return df_result

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_unidades()
    df_unidades = create_unidades_table(con, data)
    print("Table 'unidades' created successfully.")
    print(df_unidades)
