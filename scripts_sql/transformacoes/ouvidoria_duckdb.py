def create_ouvidoria_table(con):
    """
    Create the 'ouvidoria' table in DuckDB and return it as a DataFrame.
    """

    con.execute("""
        CREATE TABLE ouvidoria AS
        SELECT
            *
        FROM 
            ouvidoria_temp;
    """)

    df_ouvidoria = con.execute("SELECT * FROM ouvidoria").fetchdf()

    return df_ouvidoria
    