from ..utils.excel_operations import remove_espacos_e_acentos

def read_profissionais_equipes():
    df_servidores = remove_espacos_e_acentos('dado_bruto/profissionais_equipes/servidores.xlsx', 'servidores')
    df_equipes = remove_espacos_e_acentos('dado_bruto/profissionais_equipes/equipes.xlsx', 'equipes')
    
    return {
        'servidores_temp': df_servidores,
        'equipes_temp': df_equipes
    }
