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

def create_criancas_acompanhadas(con):
    con.execute("""
    CREATE TABLE criancas_acompanhadas AS
    SELECT 
        *
    FROM
        criancas_acompanhadas_temp
    """)

    df_criancas_acompanhadas = con.execute("SELECT * FROM criancas_acompanhadas").fetchdf()
    return df_criancas_acompanhadas

def create_criancas_atendimentos(con):
    con.execute("""
    CREATE TABLE criancas_atendimentos AS
    SELECT
        *
    FROM
        criancas_atendimentos_temp
    """)

    df_criancas_atendimentos = con.execute("SELECT * FROM criancas_atendimentos").fetchdf()
    return df_criancas_atendimentos

def create_criancas_percentual_distribuidas(con):
    con.execute("""
    CREATE TABLE criancas_percentual_distribuidas AS
    SELECT
        *
    FROM
        criancas_percentual_distribuidas_temp
    """)                                                           

    df_criancas_percentual_distribuidas = con.execute("SELECT * FROM criancas_percentual_distribuidas").fetchdf()
    return df_criancas_percentual_distribuidas

def create_criancas_percentual_acompanhadas(con):
    con.execute("""
    CREATE TABLE criancas_percentual_acompanhadas AS
    SELECT 
        *
    FROM
        criancas_percentual_acompanhadas_temp
    """)

    df_criancas_percentual_acompanhadas = con.execute("SELECT * FROM criancas_percentual_acompanhadas").fetchdf()
    return df_criancas_percentual_acompanhadas
