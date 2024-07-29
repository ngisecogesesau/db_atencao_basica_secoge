import os
import duckdb

def execute_sql_scripts(con, directory):
    """
    Executa todos os scripts SQL em um diretório usando a conexão DuckDB.
    
    :param con: Conexão DuckDB
    :param directory: Caminho para o diretório contendo os scripts SQL
    """
    for filename in os.listdir(directory):
        if filename.endswith('.sql'):
            script_path = os.path.join(directory, filename)
            with open(script_path, 'r') as file:
                sql_script = file.read()
                con.execute(sql_script)
                print(f"Script {script_path} executado com sucesso.")

def get_script_paths(script_names, base_directory='scripts_sql'):
    """
    Constrói os caminhos absolutos para uma lista de scripts SQL na pasta base especificada.
    
    :param script_names: Lista com os nomes dos arquivos de scripts SQL
    :param base_directory: Diretório base onde os scripts SQL estão localizados
    :return: Lista com os caminhos absolutos para os arquivos de scripts SQL
    """
    return [os.path.join(base_directory, script_name) for script_name in script_names]
