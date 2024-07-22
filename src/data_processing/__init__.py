from .distritos import read_distritos


def get_data_processing_functions():
    return {
        'distritos': read_distritos
    }