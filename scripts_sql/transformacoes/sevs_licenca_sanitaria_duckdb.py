def create_sevs_processos_licenciamentos_sanitarios_julho(con):
    con.execute("""
    CREATE TABLE sevs_processos_licenciamentos_sanitarios_julho AS
    SELECT
        *
    FROM
        sevs_processos_licenciamentos_sanitarios_julho_temp
    """)

    df_sevs_processos_licenciamentos_sanitarios_julho = con.execute("SELECT * FROM sevs_processos_licenciamentos_sanitarios_julho").fetchdf()
    return df_sevs_processos_licenciamentos_sanitarios_julho