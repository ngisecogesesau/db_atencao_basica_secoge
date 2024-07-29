import importlib
import os

def get_data_processing_functions():
    data_processing_functions = {}
    modules = [f[:-3] for f in os.listdir(os.path.dirname(__file__)) if f.endswith('.py') and f != '__init__.py']

    for module_name in modules:
        module = importlib.import_module(f'.{module_name}', package='src.data_processing')
        read_func_name = f'read_{module_name}'
        if hasattr(module, read_func_name):
            data_processing_functions[module_name] = getattr(module, read_func_name)

    return data_processing_functions
