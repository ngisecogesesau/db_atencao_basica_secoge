from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, Float, String, MetaData, Date
import pandas as pd
import psycopg2

def create_engine_to_db(db_name, user, password, host='localhost', port=5432):
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}')

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

def write_df_to_sql(df, table_name, engine, schema, if_exists='replace'):
    primary_key = f"id_{table_name}"
    if primary_key in df.columns:
        df = df.drop(columns=[primary_key])
    metadata = MetaData(schema=schema)
    columns = [Column(primary_key, Integer, primary_key=True, autoincrement=True)]
    for col_name, col_type in zip(df.columns, df.dtypes):
        if col_type == 'int64':
            columns.append(Column(col_name, BigInteger if df[col_name].max() > 2147483647 else Integer))
        elif col_type == 'float64':
            columns.append(Column(col_name, Float))
        elif col_type == 'datetime64[ns]':
            columns.append(Column(col_name, Date))
        else:
            columns.append(Column(col_name, String))
    table = Table(table_name, metadata, *columns)
    if if_exists == 'replace':
        table.drop(engine, checkfirst=True)
    metadata.create_all(engine)
    df.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)
