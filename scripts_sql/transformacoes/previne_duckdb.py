def create_serie_historica_previne(con):
    con.execute("""
        CREATE TABLE serie_historica_previne AS
        SELECT 
            *
        FROM
            serie_historica_previne_temp
    """)

    df_serie_historica_previne = con.execute("SELECT * FROM serie_historica_previne").fetchdf()
    return df_serie_historica_previne


def create_resultado_indicadores_desempenho_consolidado_ms(con):
    con.execute("""
        CREATE TABLE resultado_indicadores_desempenho_consolidado_ms AS
        SELECT
            *
        FROM
            resultado_indicadores_desempenho_consolidado_ms_temp
    """)

    df_resultado_indicadores_desempenho_consolidado_ms = con.execute("SELECT * FROM resultado_indicadores_desempenho_consolidado_ms").fetchdf()
    return df_resultado_indicadores_desempenho_consolidado_ms
