def create_coletas_postos_table(con):
    con.execute("""
        CREATE TABLE coletas_postos AS
        SELECT
            *
        FROM
            coletas_postos_temp
    """)

    df_coletas_postos = con.execute("SELECT * FROM coletas_postos").fetchdf()
    return df_coletas_postos

