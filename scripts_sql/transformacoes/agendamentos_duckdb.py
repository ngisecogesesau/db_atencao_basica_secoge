import duckdb
import logging
import sys
import os

from src.data_processing.agendamentos import read_agendamentos

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

def create_agendamentos_table(con):
    """
    Create the 'agendamentos' table in DuckDB and return it as a DataFrame.
    """
    
    con.execute("""
        CREATE TABLE agendamentos AS 
        SELECT 
           *
        FROM 
            servidores_agendamentos;
    """)

    df_agendamentos = con.execute("SELECT * FROM agendamentos").fetchdf()

    return df_agendamentos

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_agendamentos()
    df_agendamentos = create_agendamentos_table(con, data)

    logging.info("Table 'agendamentos' created successfully.")
    logging.info("DataFrame 'horarios':\n%s", df_agendamentos)