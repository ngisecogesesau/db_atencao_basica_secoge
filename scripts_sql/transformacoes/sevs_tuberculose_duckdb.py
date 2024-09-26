def create_sevs_tuberculose(con):
    con.execute("""
    CREATE TABLE sevs_tuberculose AS
    SELECT 
        *
    FROM
        sevs_tuberculose_temp
    """)

    df_sevs_tuberculose = con.execute("SELECT * FROM sevs_tuberculose").fetchdf()
    return df_sevs_tuberculose