import duckdb
from src.data_processing.calendario import create_calendario

def create_table(con):
    # Criando a tabela no DuckDB
    con.execute("""
    CREATE TABLE calendario (
        id_calendario INTEGER PRIMARY KEY,
        data_dma DATE,
        ano INTEGER,
        mes INTEGER,
        dia INTEGER,
        nome_dia VARCHAR,
        dia_semana INTEGER,
        mes_abreviado VARCHAR,
        quadrimestre INTEGER,
        ano_quadrimestre VARCHAR,
        mes_completo VARCHAR,
        mvm INTEGER
    )
    """)

def insert_data_calendario_table(con, df_calendario):
    # Inserindo dados no DuckDB
    con.append('calendario', df_calendario)

def create_calendario_table(con):
    # Criando a tabela
    create_table(con)
    
    # Gerando o DataFrame de calendário
    data_calendario = create_calendario()
    df_calendario = data_calendario['calendario']  # Acessando o DataFrame retornado pela função create_calendario
    
    # Inserindo dados na tabela
    insert_data_calendario_table(con, df_calendario)
    
    # Recuperando dados inseridos para retorno
    df_calendario_duckdb = con.execute("SELECT * FROM calendario").fetchdf()
    return df_calendario_duckdb

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    df_calendario_duckdb = create_calendario_table(con)
    print(df_calendario_duckdb)
