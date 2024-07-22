from ..utils.excel_operations import remove_espacos_e_acentos
from ..database import write_df_to_sql

def read_distritos(engine):
    path_asu_provisorio = 'dado_bruto/distritos/asu_provisório.xlsx'
    schema = 'asu'

    abas_asu = [
        'asu_monitora',
        'equipes_asu',
        'unidades_equipes_asu'
    ]

    for aba in abas_asu:
        df_asu_tratdo