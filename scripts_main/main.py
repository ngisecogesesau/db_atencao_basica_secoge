import os
import sys
import logging
from contextlib import contextmanager
import pandas as pd
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, create_schemas
from scripts_sql.transformacoes.unidades_duckdb import create_unidades_table, create_tipo_unidade_table
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
          
            con.register(full_table_name, df)
            logger.info(f"Registered {full_table_name} in DuckDB")


    registered_tables = con.execute("SHOW TABLES").fetchall()
    logger.info("Tabelas registradas no DuckDB: %s", registered_tables)
    
    # Passo 2: Executar transformações diretamente no DuckDB
    # Crie a tabela 'unidades' no DuckDB
    df_unidades = create_unidades_table(con, dfs)
    

    df_tipo_unidade = create_tipo_unidade_table(con)
    
    try:
        df_unidades.to_sql('unidades', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'unidades' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")
        
        df_tipo_unidade.to_sql('tipoUnidade', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'tipoUnidade' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")
        
    except Exception as e:
        logger.error(f"Erro ao processar tabelas: {e}")

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
