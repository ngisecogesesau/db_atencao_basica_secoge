
import duckdb
from src.data_processing.profissionais_equipes import read_profissionais_equipes
from src.data_processing.unidades import read_unidades

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

def update_servidores_table(con):
    con.execute(""" 
        ALTER TABLE servidores ADD COLUMN fk_id_unidades INTEGER;
        UPDATE servidores
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE servidores.cnes_unidade_de_lotacao = unidades.cnes_padrao;
""")
    
    df_update_servidores = con.execute("SELECT * FROM servidores").fetchdf()
    return df_update_servidores

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

def update_equipes_table(con):
   
    con.execute("""
                
        ALTER TABLE equipes ADD COLUMN fk_id_unidades INTEGER;
                
        UPDATE memory.equipes 
        SET fk_id_unidades = memory.unidades.id_unidades
        FROM memory.unidades
        WHERE memory.equipes.cnes = memory.unidades.cnes_padrao
    """)

    df_update_equipes = con.execute("SELECT * FROM equipes").fetchdf()
    return df_update_equipes

def create_tipo_equipe_table(con):
    """
    Create the 'tipo_equipe' table in duckdb and return it as a dataframe.
    """

    con.execute("""
        CREATE TABLE IF NOT EXISTS tipo_equipe (
                tipo_equipe INTEGER,
                fk_id_tipo_equipe INTEGER
                )
""")
    
    con.execute(""" 
        CREATE SEQUENCE id_sequence START 1;
        ALTER TABLE tipo_equipe ADD COLUMN id_tipo_equipe INTEGER DEFAULT nextval('id_sequence');
""")
    
    con.execute("""
        INSERT INTO tipo_equipe (tipo_equipe, fk_id_tipo_equipe)
        SELECT 
            tp_equipe,
            id_equipes 
        FROM equipes_temp
""")

    df_tipo_equipe = con.execute("SELECT * FROM tipo_equipe").fetchdf()
    return df_tipo_equipe


if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_profissionais_equipes()
    data_unidades = read_unidades()
    df_servidores = create_servidores_table(con, data)
    df_update_servidores = update_servidores_table(con, data_unidades)
    df_equipes = create_equipes_table(con, data)
    df_update_equipes = update_equipes_table(con, data_unidades)
    df_tipo_equipe = create_tipo_equipe_table(con, data)
    print("Table 'unidades' created successfully.")
    print(df_servidores)
    print("Table 'tipoUnidade' created successfully.")
    print(df_equipes)
    print("Table 'tipo_equipe' created successfully.")
    print(df_tipo_equipe)