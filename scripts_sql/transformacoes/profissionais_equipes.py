import duckdb

def execute_duckdb_scripts(con):
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
