def create_areas_cobertas_psa_table(con):
    con.execute("""
    CREATE TABLE areas_cobertas_psa AS
    SELECT
        *
    FROM 
        areas_cobertas_psa_temp
    LIMIT 
        9

    """)

    df_areas_cobertas_psa = con.execute("SELECT * FROM areas_cobertas_psa").fetchdf()
    return df_areas_cobertas_psa