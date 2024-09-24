import logging

def create_bd_agendamentos_table(con):
    """
    Create the 'bd_agendamentos' table in DuckDB and return it as a DataFrame.
    """

    con.execute("""
        CREATE TABLE bd_agendamentos AS
        SELECT
            *
        FROM 
            bd_agendamentos_temp;
    """)

    df_bd_agendamentos = con.execute("SELECT * FROM bd_agendamentos").fetchdf()

    return df_bd_agendamentos

def update_bd_agendamentos_table(con):

    con.execute("""
        ALTER TABLE bd_agendamentos
        ALTER COLUMN ds TYPE INTEGER USING ds::INTEGER;
    """)

    con.execute("""
            ALTER TABLE bd_agendamentos ADD COLUMN fk_id_unidades INTEGER;
            
            UPDATE bd_agendamentos
            SET fk_id_unidades = unidades.id_unidades
            FROM unidades
            WHERE unidades.cnes = bd_agendamentos.nu_cnes;
                    
            ALTER TABLE bd_agendamentos ADD COLUMN fk_id_equipes INTEGER;
            
            UPDATE bd_agendamentos
            SET fk_id_equipes = equipes.id_equipes
            FROM equipes
            WHERE bd_agendamentos.nu_ine = equipes.seq_equipe; 

            ALTER TABLE bd_agendamentos ADD COLUMN fk_id_distritos INTEGER;

            UPDATE bd_agendamentos
            SET fk_id_distritos = distritos.id_distritos
            FROM distritos
            WHERE bd_agendamentos.ds = distritos.distrito_num_inteiro;
        """)


    df_update_bd_agendamentos = con.execute("SELECT * FROM bd_agendamentos").fetchdf()
    
    return df_update_bd_agendamentos

def create_bd_agenda_configurada(con):
    con.execute("""
    CREATE TABLE bd_agenda_configurada AS
    SELECT 
        *
    FROM
        bd_agenda_configurada_temp
    """)

    df_bd_agenda_configurada = con.execute("SELECT * FROM bd_agenda_configurada").fetchdf()
    return df_bd_agenda_configurada

def create_interdicoes(con):
    con.execute("""
    CREATE TABLE interdicoes AS
    SELECT 
        *
    FROM
        interdicoes_temp
    """)

    df_interdicoes = con.execute("SELECT * FROM interdicoes_temp").fetchdf()
    return df_interdicoes