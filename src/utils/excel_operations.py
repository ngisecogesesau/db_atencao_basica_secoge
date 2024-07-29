import pandas as pd

def remove_espacos_e_acentos(df):
    if df is None:
        print("Input DataFrame is None")
        return None

    # Normalizar nomes das colunas
    df.columns = (
        df.columns
        .str.normalize('NFKD')  # Normalizar caracteres unicode
        .str.encode('ascii', errors='ignore')  # Remover acentos
        .str.decode('utf-8')
        .str.replace(r'\(a\)', '', regex=True)
        .str.replace('/', '') 
        .str.replace(' ', '_')  
        .str.lower()
    )
    
    # Debug statement to check column names
    print("Normalized column names:", df.columns.tolist())

    return df