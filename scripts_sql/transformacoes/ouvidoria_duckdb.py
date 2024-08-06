import duckdb
import logging
import sys
import os

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))

from src.data_processing.agendamentos import read_agendamentos

def create_ouvidoria_table(con):
    """
    Create the 'ouvidoria' table in DuckDB and return it as a DataFrame.
    """

    con.execute("""
        CREATE TABLE ouvidoria AS
        SELECT
            *
        FROM 
            ouvidoria_temp;
    """)

    df_ouvidoria = con.execute("SELECT * FROM ouvidoria").fetchdf()

    return df_ouvidoria


if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data = read_agendamentos()
    df_ouvidoria = data['ouvidoria']
    df_ouvidoria = create_ouvidoria_table(con)

    logging.info("Table 'ouvidoria' created successfully.")
    logging.info("DataFrame 'ouvidoria':\n%s", df_ouvidoria)
    