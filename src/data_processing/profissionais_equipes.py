from ..utils.excel_operations import remove_espacos_e_acentos
from ..database import write_df_to_sql

def read_profissionais_equipes(engine):
    path_servidores = 'dado_bruto/profissionais_equipes/servidores.xlsx'
    aba_servidores = 'servidores'
    df_servidores = remove_espacos_e_acentos(path_servidores, aba_selecionada=aba_servidores)

    path_equipes = 'data_bruto/profissionais_equipes/equipes.xlsx'
    aba_equipes = 'equipes'
    df_equipes = remove_espacos_e_acentos(path_equipes, aba_selecionada=aba_equipes)

    return {
        'servidores': df_servidores,
        'equipes': df_equipes
    }