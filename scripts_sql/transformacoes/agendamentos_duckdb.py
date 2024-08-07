import logging

def create_agendamentos_table(con):
    """
    Create the 'agendamentos' table in DuckDB and return it as a DataFrame.
    """

    con.execute("""
        CREATE TABLE agendamentos AS
        SELECT
            *
        FROM 
            agendamentos_temp;
    """)

    df_agendamentos = con.execute("SELECT * FROM agendamentos").fetchdf()

    return df_agendamentos

def update_agendamentos_table(con):
    con.execute("""
            ALTER TABLE agendamentos ADD COLUMN fk_id_unidades INTEGER;
            
            UPDATE agendamentos
            SET fk_id_unidades = unidades.id_unidades
            FROM unidades
            WHERE unidades.cnes = agendamentos.nu_cnes;
                    
            ALTER TABLE agendamentos ADD COLUMN fk_id_equipes INTEGER;
            
            UPDATE agendamentos
            SET fk_id_equipes = equipes.id_equipes
            FROM equipes
            WHERE agendamentos.nu_ine = equipes.seq_equipe; 

            ALTER TABLE agendamentos ADD COLUMN fk_id_distritos INTEGER;

            UPDATE agendamentos
            SET fk_id_distritos = distritos.id_distritos
            FROM distritos
            WHERE agendamentos.ds = distritos.sigla_distrito;
        """)


    df_update_agendamentos = con.execute("SELECT * FROM agendamentos").fetchdf()

    logging.info("Tabela 'agendamentos' atualizada com sucesso.")
    
    return df_update_agendamentos