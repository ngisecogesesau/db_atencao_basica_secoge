import os
import sys
import logging
from contextlib import contextmanager
import pandas as pd
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, create_schemas
from src.data_processing import get_data_processing_functions
from scripts_sql.transformacoes import execute_transformations_and_save

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

@contextmanager
def db_connection(config):
    engine = create_engine_to_db(**config)
    try:
        yield engine
    finally:
        engine.dispose()

def register_tables_in_duckdb(con, schemas):
    for schema_name, read_data_func in schemas.items():
        dfs = read_data_func()
        if dfs is None or not isinstance(dfs, dict):
            logger.error(f"A função {read_data_func} retornou um objeto não esperado: {type(dfs)}")
            continue
        for table_name, df in dfs.items():
            if isinstance(df, pd.Series):
                df = df.to_frame()
            full_table_name = f"{table_name}_temp"
            con.register(full_table_name, df)
            logger.info(f"Registered {full_table_name} in DuckDB")
    return con.execute("SHOW TABLES").fetchall()

def process_data(engine, schemas):
    con = duckdb.connect(database=':memory:')
    registered_tables = register_tables_in_duckdb(con, schemas)
    logger.info("Tabelas registradas no DuckDB: %s", registered_tables)
    execute_transformations_and_save(con, engine)

def main():
    config = {
        'db_name': 'db_atbasica',
        'user': 'secoge',
        'password': 'secoge',
        'host': '172.30.1.37',
        'port': 5252
    }

    create_schemas(**config)
    schemas = get_data_processing_functions()

    with db_connection(config) as engine:
        process_data(engine, schemas)

if __name__ == '__main__':
    main()