import duckdb
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data_processing.agendamentos import read_agendamentos

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
    if 'unidades' in con.execute("SHOW TABLES").fetchall():
        con.execute("""
            ALTER TABLE agendamentos ADD COLUMN fk_id_agendamentos INTEGER;
            
            UPDATE agendamentos
            SET fk_id_unidades = unidades.id_unidades
            FROM unidades
            WHERE unidades.cnes = agendamentos.nu_cnes;
                    
            ALTER TABLE agendamentos ADD COLUMN fk_id_equipes INTEGER;
            
            UPDATE agendamentos
            SET fk_id_equipes = agendamentos.id_equipes
            FROM horarios
            WHERE agendamentos.nu_ine = equipes.cnes; 

            ALTER TABLE agendamentos ADD COLUMNS fk_id_distrito INTEGER;

            UPDATE agendamentos
            SET fk_id_distritos = distritos.id_distritos
            FROM distritos
            WHERE agendamentos.ds = distritos.sigla_distrito;
                     
    """)
    else:
        logging.warning("Tabela unidades não encontrada!")

    df_update_unidades = con.execute("SELECT * FROM agendamentos").fetchdf()

    logging.info("Tabela 'agendamentos' atualizada com sucesso.")
    
    return df_update_unidades

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_agendamentos()
    df_agendamentos = create_agendamentos_table(con, data)
    df_update_agendamentos = update_agendamentos_table(con,data)

    logging.info("Table 'agendamentos' created successfully.")
    logging.info("DataFrame 'agendamentos':\n%s", df_agendamentos)