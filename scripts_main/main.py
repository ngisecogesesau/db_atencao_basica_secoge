import os
import sys
import logging
from contextlib import contextmanager
import pandas as pd
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, create_schemas
from scripts_sql.transformacoes.profissionais_equipes_duckdb import trata_df_profissionais_equipes
from src.data_processing import get_data_processing_functions

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

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
    for schema_name, read_data_func in schemas.items():
        dfs = read_data_func()
        # Verificação: Imprimir as primeiras linhas de cada DataFrame
        for table_name, df in dfs.items():
            full_table_name = f"{table_name}_temp"
            print(f"\nDataFrame for {full_table_name}:")
            print(df.head())
            # Registrar DataFrames no DuckDB
            con.register(full_table_name, df)
            logger.info(f"Registered {full_table_name} in DuckDB")

    # List the tables in DuckDB to verify registration
    registered_tables = con.execute("SHOW TABLES").fetchall()
    logger.info("Tabelas registradas no DuckDB: %s", registered_tables)
    
    # Passo 2: Executar transformações diretamente no DuckDB
    trata_df_profissionais_equipes(con)
    
    # Verificar tabelas criadas no DuckDB
    transformed_tables = con.execute("SHOW TABLES").fetchall()
    logger.info("Tabelas no DuckDB após transformação: %s", transformed_tables)
    
    # Passo 3: Carregar tabelas transformadas no PostgreSQL
    for table in transformed_tables:
        table_name = table[0]
        df_transformed = con.execute(f"SELECT * FROM {table_name}").fetchdf()
        df_transformed.to_sql(table_name, engine, schema='profissionais_equipes', if_exists='replace', index=False)
        logger.info("Tabela %s salva no banco de dados PostgreSQL com sucesso.", table_name)


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
