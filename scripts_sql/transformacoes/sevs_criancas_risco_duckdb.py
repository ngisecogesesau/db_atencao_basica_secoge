def create_criancas_risco_elegiveis(con):
    con.execute("""
    CREATE TABLE criancas_risco_elegiveis AS
    SELECT
        *
    FROM 
        criancas_risco_elegiveis_temp
    """)

    df_criancas_risco_elegiveis = con.execute("SELECT * FROM criancas_risco_elegiveis").fetchdf()
    return df_criancas_risco_elegiveis