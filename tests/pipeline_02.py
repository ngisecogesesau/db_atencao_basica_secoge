import pandas as pd
import duckdb
from sqlalchemy import create_engine
import psycopg2

# Passo 1: Ler uma planilha Excel (.xlsx) usando pandas
excel_file_path = 'dado_bruto/profissionais_equipes/servidores.xlsx'
sheet_name = 'servidores'
df_pandas = pd.read_excel(excel_file_path, sheet_name=sheet_name)

# Passo 2: Converter o DataFrame do pandas para um DataFrame do DuckDB
df_duckdb = duckdb.query('SELECT * FROM df_pandas').df()

# Passo 3: Detalhes de conexão com o PostgreSQL
db_user = 'postgres'
db_password = 'secoge'
db_host = 'localhost'
db_port = '5432'
db_name = 'db_atbasica'

# Crie a engine de conexão com o PostgreSQL
engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# Função para criar schemas
def create_schemas(db_name, user, password, host='localhost', port=5432):
    commands = ["CREATE SCHEMA IF NOT EXISTS profissionais_equipes"]
    try:
        with psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port) as conn:
            with conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
            conn.commit()
    except Exception as error:
        print(f"Erro ao criar schemas: {error}")
        raise

# Passo 4: Criar o schema se ele não existir
create_schemas(db_name, db_user, db_password, db_host, db_port)

# Nome do schema e da tabela onde os dados serão inseridos
schema_name = 'profissionais_equipes'
table_name = 'servidores'

# Passo 5: Salvar o DataFrame do DuckDB no PostgreSQL
df_duckdb.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)

print("Dados salvos no banco de dados PostgreSQL com sucesso!")
