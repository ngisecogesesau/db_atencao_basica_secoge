def create_gratificacoes_unidades(con):
    con.execute("""
        CREATE TABLE gratificacoes_unidades AS
        SELECT 
            *
        FROM
            gratificacoes_unidades_temp
    """)

    df_gratificacoes_unidades = con.execute("SELECT * FROM gratificacoes_unidades").fetchdf()
    return df_gratificacoes_unidades
