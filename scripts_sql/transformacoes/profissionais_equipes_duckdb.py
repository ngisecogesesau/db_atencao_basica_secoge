import duckdb

def trata_df_profissionais_equipes(con):
    con.execute("""
        CREATE TABLE servidores AS 
        SELECT 
           *
        FROM 
            servidores_temp
    """)
    
    con.execute("""
        CREATE TABLE equipes AS 
        SELECT 
            *
        FROM 
            equipes_temp
    """)
    
    print("Scripts DuckDB executados com sucesso.")
