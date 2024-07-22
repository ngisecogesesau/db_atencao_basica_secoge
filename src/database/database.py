from sqlalchemy import create_engine, Table, Column, Integer, BigInteger, Float, String, MetaData, text, Date
import pandas as pd
import psycopg2

def create_engine_to_db(db_name, user, password, host='localhost', port=5432):
    """
    Cria um engine SQLAlchemy para o banco de dados PostgreSQL.
    """
    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}?client_encoding=utf-8')
    return engine

def create_schemas(db_name, user, password, host='localhost', port=5432):
    """
    Cria schemas no banco de dados PostgreSQL, se não existirem.

    :param db_name: Nome do banco de dados
    :param user: Nome do usuário
    :param password: Senha do usuário
    :param host: Host do banco de dados
    :param port: Porta do banco de dados
    """
    commands = [
        "CREATE SCHEMA IF NOT EXISTS asu"
    ]

    try:
        # Conectar ao banco de dados PostgreSQL
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port,
            options="-c client_encoding=UTF8"
        )
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()

        # Executar cada comando SQL
        for command in commands:
            cur.execute(command)

        # Fechar a comunicação com o banco de dados PostgreSQL
        cur.close()
        conn.commit()
        conn.close()
        print("Schemas criados com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao criar schemas: {error}")
        raise

def write_df_to_sql(df, table_name, engine, schema, if_exists='replace'):
    """
    Escreve um DataFrame no banco de dados PostgreSQL em um esquema específico e adiciona uma coluna SERIAL como chave primária.

    :param df: DataFrame a ser escrito no banco de dados
    :param table_name: Nome da tabela onde os dados serão inseridos
    :param engine: Engine SQLAlchemy
    :param schema: Nome do esquema onde a tabela será criada
    :param if_exists: Comportamento se a tabela já existir ('replace', 'append', 'fail')
    """
    primary_key = f"id_{table_name}"

    # Remover a coluna de chave primária se ela existir no DataFrame
    if primary_key in df.columns:
        df = df.drop(columns=[primary_key])

    # Criar a tabela com SQLAlchemy
    metadata = MetaData(schema=schema)
    columns = [
        Column(primary_key, Integer, primary_key=True, autoincrement=True, nullable=False, unique=True)
    ]
    for col_name, col_type in zip(df.columns, df.dtypes):
        if col_type == 'int64':
            # Se os valores excedem o limite de Integer, use BigInteger
            if df[col_name].max() > 2147483647 or df[col_name].min() < -2147483648:
                columns.append(Column(col_name, BigInteger))
            else:
                columns.append(Column(col_name, Integer))
        elif col_type == 'float64':
            columns.append(Column(col_name, Float))
        elif col_type == 'datetime64':
            columns.append(Column(col_name, Date))
        else:
            columns.append(Column(col_name, String))

    table = Table(table_name, metadata, *columns)

    try:
        # Dropar a tabela se ela já existir
        if if_exists == 'replace':
            table.drop(engine, checkfirst=True)

        # Criar a tabela
        metadata.create_all(engine)

        # Inserir os dados no banco de dados
        df.to_sql(table_name, engine, if_exists='append', index=False, schema=schema)
        print(f"Tabela {table_name} no esquema {schema} criada e dados inseridos com sucesso.")
    except Exception as e:
        print(f"Erro ao criar a tabela {table_name} no esquema {schema} ou inserir os dados: {e}")
        raise

def connect_to_db(db_name, user, password, host='localhost', port=5432):
    """
    Conecta ao banco de dados PostgreSQL.
    """
    return create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db_name}').connect()

def execute_query(con, query):
    """
    Executa uma query SQL no banco de dados PostgreSQL.

    :param con: Objeto de conexão com o banco de dados PostgreSQL
    :param query: Query SQL a ser executada
    """
    with con.begin() as transaction:
        transaction.execute(text(query))
