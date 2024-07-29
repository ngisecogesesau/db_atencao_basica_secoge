import pandas as pd

def remove_espacos_e_acentos(df):
    df.columns = (
        df.columns
        .str.normalize('NFKD')  # Normaliza os caracteres unicode
        .str.encode('ascii', errors='ignore')  # Remove acentos
        .str.decode('utf-8')  
        .str.replace(r'\(a\)', '', regex=True)
        .str.replace('/', '') 
        #.str.replace('(', '') 
        #.str.replace(')', '') 
        .str.replace(' ', '_')  
        .str.lower()  
    )
    return df



