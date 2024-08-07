from src.data_processing.calendario import create_calendario

def create_table(con):
    """
    Create the 'calendario' table in DuckDB.
    """
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
    """
    Insert data into the 'calendario' table in DuckDB.
    """
    con.append('calendario', df_calendario)

def create_calendario_table(con):
    """
    Create and populate the 'calendario' table in DuckDB and return it as a DataFrame.
    """
    # Create the table
    create_table(con)
    
    # Generate the DataFrame for the calendar
    data_calendario = create_calendario()
    df_calendario = data_calendario['calendario']  # Accessing the DataFrame returned by the function create_calendario
    
    # Insert data into the table
    insert_data_calendario_table(con, df_calendario)
    
    # Retrieve the inserted data to return
    df_calendario_duckdb = con.execute("SELECT * FROM calendario").fetchdf()
    return df_calendario_duckdb
