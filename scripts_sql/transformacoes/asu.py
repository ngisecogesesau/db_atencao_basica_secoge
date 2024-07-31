import duckdb
from src.data_processing.asu import read_asu

def create_asu_monitora_table(con):

    con.execute("""
        CREATE TABLE asu_monitora AS
        SELECT 
            *
        FROM 
            asu_monitora_temp;
""")
    
    df_asu_monitora = con.execute("SELECT * FROM asu_monitora")
    return df_asu_monitora

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data_asu_monitora = read_asu()
    df_asu_monitora = create_asu_monitora_table(con, data_asu_monitora)
    print('Tabela asu_monitora criada com sucesso!')

