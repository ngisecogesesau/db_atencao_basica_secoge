import logging

def create_atendimentos_table(con):
    """
    Create the 'atendimentos' table in DuckDB and return it as a DataFrame.
    """
    
    con.execute("""
        CREATE TABLE atendimentos AS 
        SELECT 
           *
        FROM 
            atendimentos_temp;
    """)

    df_atendimentos = con.execute("SELECT * FROM atendimentos").fetchdf()

    return df_atendimentos

def update_atendimentos_table(con):
    if 'atendimentos' in con.execute("SHOW TABLES").fetchall():
        con.execute("""
            ALTER TABLE atendimentos ADD COLUMN fk_id_unidades INTEGER;
            
            UPDATE atendimentos
            SET fk_id_unidades = unidades.id_unidades
            FROM unidades
            WHERE unidades.cnes = atendimentos.nu_cnes;

            ALTER TABLE atendimentos ADD COLUMN fk_id_equipes INTEGER;
            
            UPDATE atendimentos
            SET fk_id_equipes = equipes.id_equipes
            FROM equipes
            WHERE equipes.seq_equipe = atendimentos.nu_ine;                  
            
  
    """)
    else:
        logging.warning("Tabela atendimentos não encontrada!")

    df_update_unidades = con.execute("SELECT * FROM atendimentos").fetchdf()

    logging.info("Tabela 'atendimentos' atualizada com sucesso.")
    
    return df_update_unidades