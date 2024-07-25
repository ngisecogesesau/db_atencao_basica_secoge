import pandas as pd

def remove_espacos_e_acentos(file_path, aba_selecionada):
    df = pd.read_excel(file_path, sheet_name=aba_selecionada)
    df.columns = df.columns.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(' ', '_').str.lower()
    return df
