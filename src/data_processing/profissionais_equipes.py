import duckdb
from ..utils.excel_operations import remove_espacos_e_acentos

def read_profissionais_equipes():
    df_servidores = remove_espacos_e_acentos('dado_bruto/profissionais_equipes/servidores.xlsx', 'servidores')
    df_equipes = remove_espacos_e_acentos('dado_bruto/profissionais_equipes/equipes.xlsx', 'equipes')

    con = duckdb.connect(database=':memory:')
    con.register('servidores', df_servidores)
    con.register('equipes', df_equipes)
    return con

def process_data_in_duckdb(con):
    return {
        'servidores': con.execute('SELECT * FROM servidores').fetchdf(),
        'equipes': con.execute('SELECT * FROM equipes').fetchdf()
    }
