import sys
import os
from contextlib import contextmanager

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, write_df_to_sql, create_schemas
from src.data_processing import get_data_processing_functions, process_data_in_duckdb

@contextmanager
def db_connection(config):
    engine = create_engine_to_db(**config)
    try:
        yield engine
    finally:
        engine.dispose()

def process_data(engine, schemas):
    for schema, read_data_func in schemas.items():
        con = read_data_func()
        processed_dfs = process_data_in_duckdb(con)
        for table_name, df in processed_dfs.items():
            write_df_to_sql(df, table_name, engine, schema)

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
