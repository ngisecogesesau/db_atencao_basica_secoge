import logging

def create_unidades_table(con):
    """
    Create 'unidades' table in DuckDB using the processed DataFrame.
    """
    con.execute("""
        CREATE TABLE unidades AS
        SELECT
            *
        FROM 
            unidades_temp;
    """)
    
    df_unidades = con.execute("SELECT * FROM unidades").fetchdf()

    logging.info("Tabela 'unidades' criada com sucesso.")
    
    return df_unidades

def create_tipo_unidade_table(con):
    """
    Create the 'tipo_unidade' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE tab_tipo_unidade AS
        SELECT
            *
        FROM 
            tipo_unidade_temp;
    """)
    
    df_tipo_unidades = con.execute("SELECT * FROM tab_tipo_unidade").fetchdf()
    
    logging.info("Tabela 'tab_tipo_unidade' criada com sucesso.")
    
    return df_tipo_unidades

def create_horarios_table(con):
    """
    Create 'horarios' table in DuckDB using the processed DataFrame.
    """
    con.execute("""
        CREATE TABLE horarios AS
        SELECT
            *
        FROM 
            horarios_temp;
    """)
    
    con.execute("""
        ALTER TABLE horarios ADD COLUMN fk_id_unidades INTEGER;
        
        UPDATE horarios
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE unidades.cnes = horarios.cnes;
                """)
    
    df_horarios = con.execute("SELECT * FROM horarios").fetchdf()
    
    logging.info("Tabela 'horarios' criada com sucesso.")
    
    return df_horarios

def create_distritos_table(con):
    """
    Create the 'distritos' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE distritos AS
        SELECT
            *
        FROM 
            distritos_temp;
    """)

    con.execute("""
        ALTER TABLE distritos
        ALTER COLUMN distrito_num_inteiro TYPE INTEGER USING distrito_num_inteiro::INTEGER;
    """)
    
    df_distritos = con.execute("SELECT * FROM distritos").fetchdf()
    
    logging.info("Tabela 'distritos' criada com sucesso.")
    
    return df_distritos

def update_unidades_table(con):
    con.execute("""
            ALTER TABLE unidades ADD COLUMN fk_id_tipo_unidade INTEGER;
            
            UPDATE unidades
            SET fk_id_tipo_unidade = tab_tipo_unidade.id_tipo_unidade
            FROM tab_tipo_unidade
            WHERE unidades.tipo_unidade = tab_tipo_unidade.tipo_unidade;
                    
            ALTER TABLE unidades ADD COLUMN fk_id_horarios INTEGER;
            
            UPDATE unidades
            SET fk_id_horarios = horarios.id_horarios
            FROM horarios
            WHERE unidades.horario = horarios.horario;     
                    
            ALTER TABLE unidades ADD COLUMN fk_id_distritos INTEGER;
            
            UPDATE unidades
            SET fk_id_distritos = distritos.id_distritos
            FROM distritos
            WHERE unidades.distrito = sigla_distrito;   
    """)

    df_update_unidades = con.execute("SELECT * FROM unidades").fetchdf()

    logging.info("Tabela 'unidades' atualizada com sucesso.")
    
    return df_update_unidades

def create_login_senha_ds_table(con):
    """
    Create the 'login_senha_ds' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE login_senha_ds AS
        SELECT
            *
        FROM 
            login_senha_ds_temp;
    """)
    
    df_login_senha_ds = con.execute("SELECT * FROM login_senha_ds").fetchdf()
    
    logging.info("Tabela 'login_senha_ds' criada com sucesso.")

    return df_login_senha_ds

def create_login_senha_unidades_table(con):
    """
    Create the 'login_senha_unidades' table in DuckDB and return it as a DataFrame.
    """
    con.execute("""
        CREATE TABLE login_senha_unidades AS
        SELECT
            *
        FROM 
            login_senha_unidades_temp;
    """)
    
    df_login_senha_ds = con.execute("SELECT * FROM login_senha_unidades").fetchdf()
    
    logging.info("Tabela 'login_senha_unidades' criada com sucesso.")

    return df_login_senha_ds
 
