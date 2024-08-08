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
        WHERE servidores.cnes_unidade_de_lotacao = unidades.cnes;
                
        ALTER TABLE servidores ADD COLUMN fk_id_distritos INTEGER;

        UPDATE servidores
        SET fk_id_distritos = distritos.id_distritos
        FROM distritos
        WHERE servidores.distrito = distritos.sigla_distrito;
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
                
        UPDATE equipes 
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE equipes.cnes = unidades.cnes;
                
        ALTER TABLE equipes ADD COLUMN fk_id_distritos INTEGER;

        UPDATE equipes
        SET fk_id_distritos = unidades.fk_id_distritos
        FROM unidades
        WHERE fk_id_unidades = id_unidades;

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

def create_equipes_usf_mais_table(con):
    """
    Create the 'equipes_usf_mais' table in duckdb and return it as a dataframe.
    """

    con.execute("""
        CREATE TABLE equipes_usf_mais AS
        SELECT
            *
        FROM
            equipes_usf_mais_temp           
    """)

    df_equipes_usf_mais = con.execute("SELECT * FROM equipes_usf_mais").fetchdf()
    return df_equipes_usf_mais

def update_equipes_usf_mais_table(con):
   
    con.execute("""
                
        ALTER TABLE equipes_usf_mais ADD COLUMN fk_id_unidades INTEGER;
                
        UPDATE equipes_usf_mais 
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE equipes_usf_mais.cnes = unidades.cnes;
                
        ALTER TABLE equipes_usf_mais ADD COLUMN fk_id_distritos INTEGER;
                
        UPDATE equipes_usf_mais 
        SET fk_id_distritos = distritos.id_distritos
        FROM distritos
        WHERE equipes_usf_mais.distrito = distritos.sigla_distrito;
    """)

    df_update_equipes_usf_mais = con.execute("SELECT * FROM equipes_usf_mais").fetchdf()
    return df_update_equipes_usf_mais

def create_gerentes_table(con):
    """
    Cria tabela gerentes no duckdb e retorna um dataframe duckdb
    """

    con.execute("""
        CREATE TABLE gerentes AS
        SELECT 
            *
        FROM
            gerentes_temp
""")
    
    df_gerentes = con.execute("SELECT * FROM gerentes").fetchdf()
    return df_gerentes

def update_gerentes_table(con):
   
    con.execute("""
                
        ALTER TABLE gerentes ADD COLUMN fk_id_unidades INTEGER;
                
        UPDATE gerentes 
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE gerentes.cnes = unidades.cnes;  

        ALTER TABLE gerentes ADD COLUMN fk_id_distritos INTEGER;
                
        UPDATE gerentes 
        SET fk_id_distritos = distritos.id_distritos
        FROM distritos
        WHERE gerentes.ds = distritos.id_distritos;        
            
    """)

    df_update_gerentes = con.execute("SELECT * FROM gerentes").fetchdf()
    return df_update_gerentes

    