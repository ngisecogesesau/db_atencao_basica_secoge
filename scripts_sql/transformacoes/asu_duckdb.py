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
    
    df_asu_monitora = con.execute("SELECT * FROM asu_monitora").fetchdf()
    return df_asu_monitora

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
    

if __name__ == '__main__':
    con = duckdb.connect(database=':memory:')
    data_asu = read_asu()
    df_asu_monitora = create_asu_monitora_table(con, data_asu)
    df_asu_classificacao = create_asu_classificacao_table(con, data_asu)
    print('Tabelas asu criada com sucesso!')