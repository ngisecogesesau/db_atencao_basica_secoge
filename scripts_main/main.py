import os
import sys
import logging
from contextlib import contextmanager
import pandas as pd
import duckdb

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.database import create_engine_to_db, create_schemas
from scripts_sql.transformacoes.profissionais_equipes_duckdb import create_servidores_table, create_equipes_table, create_tipo_equipe_table, update_equipes_table, update_servidores_table
from scripts_sql.transformacoes.unidades_duckdb import create_unidades_table, create_tipo_unidade_table, create_horarios_table, create_info_unidades_table, create_distritos_table, update_unidades_table
from scripts_sql.transformacoes.asu_duckdb import create_asu_classificacao_table, create_asu_monitora_table
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
    # Tabelas do schema unidades
    df_unidades = create_unidades_table(con)
    df_tipo_unidade = create_tipo_unidade_table(con)
    df_horarios = create_horarios_table(con)
    df_info_unidades = create_info_unidades_table(con)
    df_distritos = create_distritos_table(con)
    df_update_unidades = update_unidades_table(con)

    # Tabelas schema profissionais_equipes
    # Cria tabela servidores no DuckDB
    df_servidores = create_servidores_table(con)
    # Atualiza tabela servidores do DuckDB
    df_update_servidores = update_servidores_table(con)
    # Cria tabela equipes no DuckDB
    df_equipes = create_equipes_table(con)
    # Atualiza tabela equipes
    df_update_equipes = update_equipes_table(con)
    # Cria tabela tipo_equipe no DuckDb
    df_tipo_equipe = create_tipo_equipe_table(con)

    # Tabelas schemas asu
    # Cria tabela asu_monitora
    df_asu_monitora = create_asu_monitora_table(con)
    # Cria tabela asu_classificacao
    df_asu_classificacao = create_asu_classificacao_table(con)

    try:
        df_unidades.to_sql('unidades', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'unidades' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")
        
        df_tipo_unidade.to_sql('tipoUnidade', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'tipoUnidade' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")

        df_horarios.to_sql('horarios', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'horarios' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")
        
        df_info_unidades.to_sql('info_unidades', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'inf_unidades' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")

        df_distritos.to_sql('distritos', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'distritos' salva no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")
        
        df_servidores.to_sql('servidores', engine, schema='profissionais_equipes', if_exists='replace', index=False)
        logger.info("Tabela 'servidores' salva no esquema 'profissionais_equipes' do banco de dados PostgreSQL com sucesso.")

        df_update_servidores.to_sql('servidores', engine, schema='profissionais_equipes', if_exists='replace', index=False)
        logger.info("Tabela 'serviodres' atualizada no esquema 'profissionais_equipes' do banco de dados PostgreSQL com sucesso.")

        df_equipes.to_sql('equipes', engine, schema='profissionais_equipes', if_exists='replace', index=False)
        logger.info("Tabela 'equipes' salva no esquema 'profissionais_equipes' do banco de dados PostgreSQL com sucesso.")

        df_update_equipes.to_sql('equipes', engine, schema='profissionais_equipes', if_exists='replace', index=False)
        logger.info("Tabela 'equipes' atualizada no esquema 'profissionais_equipes' do banco de dados PostgreSQL com sucesso.")

        df_tipo_equipe.to_sql('tipo_equipe', engine, schema='profissionais_equipes', if_exists='replace', index=False)
        logger.info("Tabela 'tipo_equipe' salva no esquema 'profissionais_equipes' do banco de dados PostgreSQL com sucesso.")
        
        df_update_unidades.to_sql('unidades', engine, schema='unidades', if_exists='replace', index=False)
        logger.info("Tabela 'unidades' atualizada no esquema 'unidades' do banco de dados PostgreSQL com sucesso.")

        df_asu_monitora.to_sql('asu_monitora', engine, schema='asu', if_exists='replace', index=False)
        logger.info("Tabela 'asu_monitora' criada no esquema 'asu' do banco de dados PostgreSQL com sucesso.")

        df_asu_classificacao.to_sql('asu_classificacao', engine, schema='asu', if_exists='replace', index=False)
        logger.info("Tabela 'asu_classificacao' criada no esquema 'asu' do banco de dados PostgreSQL com sucesso.")

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