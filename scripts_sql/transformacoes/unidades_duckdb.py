import sys
import os
import duckdb
import pandas as pd
import logging

# Adicionar o caminho do diretório src ao sys.path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '../../src')))

# Importar DataFrames do módulo unidades
from data_processing.unidades import globals

# Configuração do logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Conectar ao banco DuckDB
conn = duckdb.connect('data/transformed_data.duckdb')  # Certifique-se de que o caminho esteja correto para o seu banco de dados

# Criar a tabela 'unidades' com uma coluna 'id_unidades' SERIAL
create_table_sql = """
CREATE TABLE IF NOT EXISTS unidades (
    id_unidades INTEGER PRIMARY KEY,
    CNES INTEGER,
    CNES_PADRAO INTEGER
    -- Adicione outras colunas necessárias aqui
);
"""

conn.execute(create_table_sql)

# Função para carregar e transformar os DataFrames
def load_and_transform_dataframes():
    # Obter DataFrames com prefixos 'df_usf_' e 'df_unidades_'
    df_usf_vars = [globals()[var_name] for var_name in globals() if var_name.startswith('df_usf_')]
    df_unidades_vars = [globals()[var_name] for var_name in globals() if var_name.startswith('df_unidades_')]

    # Exemplo: Unir e transformar dados conforme necessário
    # Ajuste a lógica conforme seus requisitos
    if df_usf_vars:
        usf_df = pd.concat(df_usf_vars, ignore_index=True)
        # Suponha que a coluna 'CNES' exista nos DataFrames e é usada para o 'id_unidades'
        usf_df['id_unidades'] = usf_df['CNES'].astype(int)

    if df_unidades_vars:
        unidades_df = pd.concat(df_unidades_vars, ignore_index=True)
        # Suponha que a coluna 'CNES' exista nos DataFrames e é usada para o 'id_unidades'
        unidades_df['id_unidades'] = unidades_df['CNES'].astype(int)

    return usf_df, unidades_df

# Carregar e transformar dados
usf_df, unidades_df = load_and_transform_dataframes()

# Inserir dados na tabela 'unidades'
if not usf_df.empty:
    usf_df.to_sql('unidades', conn, if_exists='replace', index=False)

if not unidades_df.empty:
    unidades_df.to_sql('unidades', conn, if_exists='replace', index=False)

logger.info("Dados inseridos na tabela 'unidades' com sucesso.")

# Fechar a conexão
conn.close()
