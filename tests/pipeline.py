import sys
import os
from contextlib import contextmanager
import pandas as pd
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, create_schemas
from src.data_processing import get_data_processing_functions

@contextmanager
def db_connection(config):
    engine = create_engine_to_db(**config)
    try:
        yield engine
    finally:
        engine.dispose()

def process_data(engine, schemas):
    con = duckdb.connect(database=':memory:')
    
    # Passo 1: Ler dados usando pandas
    for read_data_func in schemas.values():
        dfs = read_data_func()
        for table_name, df in dfs.items():
            # Registrar DataFrames no DuckDB
            con.register(table_name, df)
    
    # Passo 2: Executar transformações diretamente no DuckDB
    con.execute("""
        CREATE TABLE profissionais_equipes.servidores AS 
        SELECT 
            nome_servidor,
            situacao_funcional
        FROM 
            servidores_temp
    """)
    
    # Passo 3: Carregar tabelas transformadas no PostgreSQL
    transformed_tables = con.execute("SHOW TABLES").fetchall()
    for table in transformed_tables:
        table_name = table[0]
        if table_name not in ['servidores_temp', 'equipes_temp']:
            df_transformed = con.execute(f"SELECT * FROM {table_name}").fetchdf()
            schema_name = 'profissionais_equipes'
            df_transformed.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
            print(f"Tabela {table_name} salva no banco de dados PostgreSQL com sucesso.")

def main():
    config = {
        'db_name': 'db_atbasica',
        'user': 'postgres',
        'password': 'secoge',
        'host': 'localhost',
        'port': 5432
    }

    # Criar esquema no PostgreSQL
    create_schemas(**config)
    schemas = get_data_processing_functions()

    with db_connection(config) as engine:
        process_data(engine, schemas)

if __name__ == '__main__':
    main()
