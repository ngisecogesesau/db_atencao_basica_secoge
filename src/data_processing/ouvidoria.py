import pandas as pd
import logging

from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_ouvidoria():
    """
    Ler e processar dados para a tabela de 'ouvidoria' a partir de uma planilha.

    :return: Um DataFrame processado para 'ouvidoria'
    """
    relative_url_agendamentos = "/Shared Documents/SESAU/NGI/ouvidoria/Ouvidoria.xlsx"

    df_dict = get_file_as_dataframes(relative_url_agendamentos)

    if 'Planilha1' not in df_dict:
        logging.error("Aba 'Planilha1' não encontrada no arquivo Excel.")
        return pd.DataFrame()
    
    df_ouvidoria = df_dict['Planilha1']

    required_columns = [
        'PROTOCOLO', 'DATA DA DEMANDA', 'OUVIDORIA DE ORIGEM', 'MEIO DE ATENDIMENTO', 
        'ORIGEM DO ATENDIMENTO', 'DEMANDA ATIVA?', 'STATUS', 'DATA DE FECHAMENTO DA DEMANDA', 
        'DATA DE CONCLUSÃO EFETIVA', 'DIAS DE TRAMITACAO', 'PRAZO VENCIDO?', 'DATA DE CONCLUSÃO PREVISTA', 
        'CLASSIFICACAO', 'ASSUNTO', 'SUBASSUNTO 1', 'SUBASSUNTO 2', 'SUBASSUNTO 3', 'FARMACO', 'DAPS', 
        'Nº da US', 'ESTAB COMERCIAL', 'DS', 'PRIMEIRO DESTINO', 'DATA PRIMEIRO DESTINO ENCAMINHAMENTO', 
        'MUN PRIMEIRO DESTINO', 'UF PRIMEIRO DESTINO', 'ESFERA PRIMEIRO DESTINO', 
        'OUVIDORIA SEGUNDO ENCAMINHAMENTO', 'OUVIDORIA TERCEIRO ENCAMINHAMENTO', 'DESTINO ATUAL', 
        'MUN DESTINO ATUAL', 'UF DESTINO ATUAL', 'ESFERA DESTINO ATUAL', 'DATA DO ACOMP ATUAL', 
        'MUNICIPIO CIDADAO', 'UF CIDADAO', 'SIGILOSO?', 'ANONIMO?', 'DETALHE DA DEMANDA', 'ACOMP COMENTÁRIO'
    ]
 
    df_ouvidoria = df_ouvidoria[required_columns]
    df_ouvidoria = remove_espacos_e_acentos(df_ouvidoria)
    df_ouvidoria = add_pk(df_ouvidoria, 'ouvidorias')

    return {
        'ouvidoria': df_ouvidoria,
    }
