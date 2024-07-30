import pandas as pd

def add_pk(df, table_name):
    """
    Adiciona uma coluna de chave primária autoincrement a um DataFrame.

    :param df: DataFrame ao qual a coluna de chave primária será adicionada
    :param table_name: Nome da tabela que será usada para criar o nome da coluna de chave primária
    :return: DataFrame com a coluna de chave primária adicionada
    """
    primary_key = f"id_{table_name}"

    # Remover a coluna de chave primária se ela existir no DataFrame
    if primary_key in df.columns:
        df = df.drop(columns=[primary_key])

    # Adicionar a coluna de chave primária ao DataFrame
    df.insert(0, primary_key, range(1, len(df) + 1))

    return df