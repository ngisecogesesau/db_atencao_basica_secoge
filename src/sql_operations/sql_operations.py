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
