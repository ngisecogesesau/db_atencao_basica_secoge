from sqlalchemy import create_engine
import psycopg2

def create_engine_to_db(db_name, user, password, host='localhost', port=5432):
    return create_engine(f'postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}')

def create_schemas(db_name, user, password, host='localhost', port=5432):
    commands = ["CREATE SCHEMA IF NOT EXISTS profissionais_equipes",
                "CREATE SCHEMA IF NOT EXISTS unidades",
                "CREATE SCHEMA IF NOT EXISTS asu"
                ]
    try:
        with psycopg2.connect(dbname=db_name, user=user, password=password, host=host, port=port) as conn:
            with conn.cursor() as cur:
                for command in commands:
                    cur.execute(command)
            conn.commit()
    except Exception as error:
        print(f"Erro ao criar schemas: {error}")
        raise

