def create_asu_monitora_table(con):

    con.execute("""
        CREATE TABLE asu_monitora AS
        SELECT 
            *
        FROM 
            asu_monitora_temp;
    """)
    
    df_asu_monitora = con.execute("SELECT * FROM asu_monitora").fetchdf()
    return df_asu_monitora

def update_asu_monitora_table(con):
    con.execute("""
        ALTER TABLE asu_monitora ADD COLUMN fk_id_equipes INTEGER;
                
        UPDATE asu_monitora 
        SET fk_id_equipes = equipes.id_equipes 
        FROM equipes 
        WHERE asu_monitora.ine = equipes.seq_equipe;
        
    """)
    
    df_update_monitora_table = con.execute("SELECT * FROM asu_monitora").fetchdf()
    return df_update_monitora_table

def create_asu_classificacao_table(con):

    con.execute("""
        CREATE TABLE asu_classificacao AS
        SELECT 
            *
        FROM 
            asu_classificacao_temp;
    """)
    
    df_asu_classificacao = con.execute("SELECT * FROM asu_classificacao").fetchdf()
    return df_asu_classificacao

def create_equipes_asu_table(con):
    con.execute("""
        CREATE TABLE equipes_asu AS
            SELECT
                *
            FROM
                equipes_asu_temp;
    """)
    
    df_equipes_asu = con.execute("SELECT * FROM equipes_asu").fetchdf()
    return df_equipes_asu

def update_equipes_asu_relacionamento_equipes(con):
    con.execute("""
        ALTER TABLE equipes_asu ADD COLUMN fk_id_equipes INTEGER;
                
        UPDATE equipes_asu 
        SET fk_id_equipes = equipes.id_equipes 
        FROM equipes 
        WHERE equipes_asu.seq_equipe = equipes.seq_equipe;
        
    """)
    
    df_update_equipes_asu = con.execute("SELECT * FROM equipes_asu").fetchdf()
    return df_update_equipes_asu

def update_equipes_asu_relacionamento_unidades(con):
   
    con.execute("""
                
        ALTER TABLE equipes_asu ADD COLUMN fk_id_unidades INTEGER;
                
        UPDATE equipes_asu 
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE equipes_asu.cnes = unidades.cnes;
    """)

    df_update_equipes_asu = con.execute("SELECT * FROM equipes_asu").fetchdf()
    return df_update_equipes_asu

def create_unidades_equipes_asu(con):

    con.execute("""
        CREATE TABLE unidades_equipes_asu AS
            SELECT 
                *
            FROM 
                unidades_equipes_asu_temp;
    """)
    
    df_unidades_equipes_asu = con.execute("SELECT * FROM unidades_equipes_asu").fetchdf()
    return df_unidades_equipes_asu

def update_unidades_quipes_asu_relacionamento_unidades(con):
   
    con.execute("""
                
        ALTER TABLE unidades_equipes_asu ADD COLUMN fk_id_unidades INTEGER;
                
        UPDATE unidades_equipes_asu 
        SET fk_id_unidades = unidades.id_unidades
        FROM unidades
        WHERE unidades_equipes_asu.cnes = unidades.cnes;
    """)

    df_update_unidades_equipes_asu = con.execute("SELECT * FROM unidades_equipes_asu").fetchdf()
    return df_update_unidades_equipes_asu
