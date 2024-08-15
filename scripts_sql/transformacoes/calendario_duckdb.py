from src.data_processing.calendario import create_calendario

def create_table(con):

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

    con.append('calendario', df_calendario)

def create_calendario_table(con):

    create_table(con)
    
    data_calendario = create_calendario()
    df_calendario = data_calendario['calendario']  
    
    insert_data_calendario_table(con, df_calendario)
    
    df_calendario_duckdb = con.execute("SELECT * FROM calendario").fetchdf()
    return df_calendario_duckdb
