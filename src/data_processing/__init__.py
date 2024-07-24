from .profissionais_equipes import read_profissionais_equipes


def get_data_processing_functions():
    return {
        'profissionais_equipes': read_profissionais_equipes
    }