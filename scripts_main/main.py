import os
import sys
import logging
from contextlib import contextmanager
import pandas as pd
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, create_schemas
from src.data_processing import get_data_processing_functions
from scripts_sql.transformacoes.profissionais_equipes_duckdb import (
    create_servidores_table, create_equipes_table, create_tipo_equipe_table,
    update_equipes_table, update_servidores_table, create_equipes_usf_mais_table,
    create_gerentes_table, update_gerentes_table, update_equipes_usf_mais_table
)
from scripts_sql.transformacoes.unidades_duckdb import (
    create_unidades_table, create_tipo_unidade_table, create_horarios_table,
    create_info_unidades_table, create_distritos_table, update_unidades_table
)
from scripts_sql.transformacoes.asu_duckdb import (
    create_asu_classificacao_table, create_asu_monitora_table,
    create_equipes_asu_table, create_unidades_equipes_asu,
    update_asu_monitora_table, update_equipes_asu_relacionamento_equipes,
    update_equipes_asu_relacionamento_unidades, update_unidades_quipes_asu_relacionamento_unidades
)
from scripts_sql.transformacoes.agendamentos_duckdb import create_agendamentos_table, update_agendamentos_table
from scripts_sql.transformacoes.atendimentos_duckdb import create_atendimentos_table, update_atendimentos_table
from scripts_sql.transformacoes.calendario_duckdb import create_calendario_table

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

def save_table_to_postgres(df, table_name, engine, schema_name):
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        logger.info(f"Tabela '{table_name}' salva no esquema '{schema_name}' do banco de dados PostgreSQL com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar tabela '{table_name}': {e}")

def execute_transformations_and_save(con, engine):
    transformations = [
        (create_unidades_table, 'unidades', 'unidades'),
        (create_tipo_unidade_table, 'tipoUnidade', 'unidades'),
        (create_horarios_table, 'horarios', 'unidades'),
        (create_info_unidades_table, 'info_unidades', 'unidades'),
        (create_distritos_table, 'distritos', 'unidades'),
        (update_unidades_table, 'unidades', 'unidades'),

        (create_servidores_table, 'servidores', 'profissionais_equipes'),
        (update_servidores_table, 'servidores', 'profissionais_equipes'),
        (create_equipes_table, 'equipes', 'profissionais_equipes'),
        (update_equipes_table, 'equipes', 'profissionais_equipes'),
        (create_tipo_equipe_table, 'tipo_equipe', 'profissionais_equipes'),
        (create_equipes_usf_mais_table, 'equipes_usf_mais', 'profissionais_equipes'),
        (update_equipes_usf_mais_table, 'equipes_usf_mais', 'profissionais_equipes'),
        (create_gerentes_table, 'gerentes', 'profissionais_equipes'),
        (update_gerentes_table, 'gerentes', 'profissionais_equipes'),

        (create_asu_monitora_table, 'asu_monitora', 'asu'),
        (update_asu_monitora_table, 'asu_monitora', 'asu'),
        (create_asu_classificacao_table, 'asu_classificacao', 'asu'),
        (create_equipes_asu_table, 'equipes_asu', 'asu'),
        (update_equipes_asu_relacionamento_equipes, 'equipes_asu', 'asu'),
        (update_equipes_asu_relacionamento_unidades, 'equipes_asu', 'asu'),
        (create_unidades_equipes_asu, 'unidades_equipes_asu', 'asu'),
        (update_unidades_quipes_asu_relacionamento_unidades, 'unidades_equipes_asu', 'asu'),

        (create_agendamentos_table, 'agendamentos', 'agendamentos'),
        (update_agendamentos_table, 'agendamentos', 'agendamentos'),

        (create_atendimentos_table, 'atendimentos', 'atendimentos'),
        (update_atendimentos_table, 'atendimentos', 'atendimentos'),

        (create_calendario_table, 'calendario', 'calendario'),
    ]

    for transformation_func, table_name, schema_name in transformations:
        df = transformation_func(con)
        save_table_to_postgres(df, table_name, engine, schema_name)

def process_data(engine, schemas):
    con = duckdb.connect(database=':memory:')
    registered_tables = register_tables_in_duckdb(con, schemas)
    logger.info("Tabelas registradas no DuckDB: %s", registered_tables)
    execute_transformations_and_save(con, engine)

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
