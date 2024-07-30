
import duckdb
from src.data_processing.profissionais_equipes import read_profissionais_equipes

def create_servidores_table(con):
    """
    Create the 'servidores' table in DuckDB and return it as a DataFrame.
    """
    
    con.execute("""
        CREATE TABLE servidores AS 
        SELECT 
           *
        FROM 
            servidores_temp;
    """)

    df_servidores = con.execute("SELECT * FROM servidores").fetchdf()

    return df_servidores

def create_equipes_table(con):
    """
    Create the 'equipes' table in duckdb and return it as a dataframe.
    """

    con.execute("""
        CREATE TABLE equipes AS
        SELECT 
            *
        FROM
            equipes_temp;
""")
    
    df_equipes = con.execute("SELECT * FROM equipes").fetchdf()

    return df_equipes

def create_tipo_equipe_table(con):
    """
    Create the 'tipo_equipe' table in duckdb and return it as a dataframe.
    """

    con.execute("""
        CREATE TABLE IF NOT EXISTS tipo_equipe (
                tipo_equipe INTEGER
                )
""")
    
    con.execute("""
        INSERT INTO tipo_equipe (tipo_equipe)
        SELECT tp_equipe 
        FROM equipes_temp
""")

    df_tipo_equipe = con.execute("SELECT * FROM tipo_equipe").fetchdf()
    return df_tipo_equipe


if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_profissionais_equipes()
    df_servidores = create_servidores_table(con, data)
    df_equipes = create_equipes_table(con, data)
    df_tipo_equipe = create_tipo_equipe_table(con, data)
    print("Table 'unidades' created successfully.")
    print(df_servidores)
    print("Table 'tipoUnidade' created successfully.")
    print(df_equipes)
    print("Table 'tipo_equipe' created successfully.")
    print(df_tipo_equipe)