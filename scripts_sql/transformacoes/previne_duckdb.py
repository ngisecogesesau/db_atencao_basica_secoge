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
