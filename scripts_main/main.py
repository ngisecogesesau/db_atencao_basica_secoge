import sys
import os
from contextlib import contextmanager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, write_df_to_sql, create_schemas
from src.data_processing import get_data_processing_functions
from src.sql_operations.sql_operations import execute_sql_scripts
import duckdb

@contextmanager
def db_connection(config):
    engine = create_engine_to_db(**config)
    try:
        yield engine
    finally:
        engine.dispose()

def configure_duckdb():
    con = duckdb.connect(database=':memory:')
    con.execute("INSTALL spatial;")
    con.execute("LOAD spatial;")
    return con

def process_data(engine, schemas):
    con = configure_duckdb()
    
    for schema, read_data_func in schemas.items():
        dfs = read_data_func()
        for table_name, df in dfs.items():
            con.register(table_name, df)
    
    # Executa scripts SQL de tratamento
    execute_sql_scripts(con, 'scripts_sql/transformacoes')

    # Carrega tabelas tratadas no PostgreSQL
    for table_name in con.execute("SHOW TABLES").fetchall():
        df = con.execute(f"SELECT * FROM {table_name[0]}").fetchdf()
        write_df_to_sql(df, table_name[0], engine, schema)

def main():
    config = {
        'db_name': 'db_atbasica',
        'user': 'postgres',
        'password': 'secoge',
        'host': 'localhost',
        'port': 5432
    }

    create_schemas(**config)
    schemas = get_data_processing_functions()

    with db_connection(config) as engine:
        process_data(engine, schemas)

if __name__ == '__main__':
    main()
