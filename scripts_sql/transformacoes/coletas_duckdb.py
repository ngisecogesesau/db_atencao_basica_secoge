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

def create_dados_qualidade_coleta_laboratorio_clinica(con):
    con.execute("""
    CREATE TABLE dados_qualidade_coleta_laboratorio_clinica AS
    FROM
        *
        dados_qualidade_coleta_laboratorio_clinica_temp
    """)

    df_dados_qualidade_coleta_laboratorio_clinica = con.execute("SELECT * FROM dados_qualidade_coleta_laboratorio_clinica").fetchdf()

    return df_dados_qualidade_coleta_laboratorio_clinica