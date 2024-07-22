import os
import psycopg2

def execute_sql_script(db_name, user, password, host, port, script_path):
    """
    Executa scripts SQL a partir dos arquivos.
    
    :param db_name: Nome do banco de dados
    :param user: Nome do usuário
    :param password: Senha do usuário
    :param host: Host do banco de dados
    :param port: Porta do banco de dados
    :param script_path: Caminho para o arquivo SQL
    """
    script_path = os.path.abspath(script_path)  # Converte para caminho absoluto

    try:
        # Conectar ao banco de dados PostgreSQL
        conn = psycopg2.connect(
            dbname=db_name,
            user=user,
            password=password,
            host=host,
            port=port,
            options="-c client_encoding=UTF8"
        )
        conn.set_client_encoding('UTF8')
        cur = conn.cursor()

        # Ler o script SQL
        with open(script_path, 'r') as file:
            script = file.read()

        # Executar o script SQL
        cur.execute(script)

        # Fechar a comunicação com o banco de dados PostgreSQL
        cur.close()
        conn.commit()
        conn.close()
        print(f"Script {script_path} executado com sucesso.")
    except (Exception, psycopg2.DatabaseError) as error:
        print(f"Erro ao executar o script {script_path}: {error}")
        raise

def get_script_path(script_names):
    """
    Constrói os caminhos absolutos para uma lista de scripts SQL na pasta scripts_sql.
    
    :param script_names: Lista com os nomes dos arquivos de scripts SQL
    :return: Lista com os caminhos absolutos para os arquivos de scripts SQL
    """
    # Lista para armazenar os caminhos absolutos
    script_paths = []
    for script_name in script_names:
        # Caminho absoluto para cada arquivo de script SQL.
        script_paths.append(os.path.join('scripts_sql', script_name))
    return script_paths

