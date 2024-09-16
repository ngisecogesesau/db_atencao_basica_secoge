from src.utils.extract_sharepoint_df import get_file_as_dataframes
from src.utils.excel_operations import remove_espacos_e_acentos
from src.utils.add_primary_key import add_pk

def read_profissionais_equipes():
    """
    Ler e processar dados para 'servidores' e 'equipes'.

    :return: Um dicionário com DataFrames processados para 'servidores' e 'equipes'
    """

    # Tabela EQUIPES_CNES (nome original)
    url_equipes_cnes = '/Shared Documents/SESAU/NGI/unidades/dados_estabelecimento_equipes_cnes_mar.xlsx'
    df_equipes_cnes = get_file_as_dataframes(url_equipes_cnes)

    # df_equipes é a nova nomenclatura apos normalizacao
    df_equipes_cnes = df_equipes_cnes['in']
    df_equipes_columns = ['CNES', 'SEQ_EQUIPE', 'DS_EQUIPE', 'NM_REFERENCIA', 'TURNO_ATEND', 'CRIACAO_EQUIPE', 'DT_DESATIVACAO', 
                            'ID_TP_EQUIPE', 'TP_EQUIPE', 'SG_EQUIPE' ]
    
    df_equipes = df_equipes_cnes[df_equipes_columns]
    df_equipes = remove_espacos_e_acentos(df_equipes)
    df_equipes = add_pk(df_equipes, 'equipes')

    # Tabela USF (nome original)
    url_usf = "/Shared Documents/SESAU/NGI/unidades/USF.xlsx"
    df_usf = get_file_as_dataframes(url_usf)

    usf_columns = ['NOME DO SERVIDOR(A)', 'SITUAÇAO FUNCIONAL', 'PERFIL DO CARGO',  'CODIGO UNIDADE DE LOTAÇAO', 
                   'PERFIL UNIDADE DE LOTACAO', 'DISTRITO', 'EQUIPE', 'CNES UNIDADE DE LOTAÇAO', 
                    'SETOR', 'TURNO DE TRABALHO']  
    
    # df_servidores é a nova nomenclatura apos normalizacao
    df_usf = df_usf['USF']
    df_servidores = df_usf[usf_columns]
    df_servidores = remove_espacos_e_acentos(df_servidores)
    df_servidores = add_pk(df_servidores, 'servidores')

    # Tabela USF + (nome_original)
    url_usf_mais = "/Shared Documents/SESAU/NGI/unidades/ANALISE_PEAB_USF_29.02_DemandaBI.xlsx"
    df_usf_mais = get_file_as_dataframes(url_usf_mais)

    usf_mais_columns = ['CNES', 'DISTRITO', 'Nº DA ESF', 'TURNO DA ESF', 'HORÁRIO DA ESF', 'MÉDICO DA ESF', 'ENFERMEIRO ESF', 
                        'TÉCNICO ESF', 'ACS', 'Nº ESB', 'TURNO ESB', 'HORÁRIO ESB', 'CIR. DENTISTA', 'ASB', 'RECEPCIONISTA', 
                        'TURNO DO RECEPCIONISTA', 'HORÁRIO DO RECEPCIONISTA', 'REGULAÇÃO', 'TURNO DO PROF. REGULAÇÃO', 'HORÁRIO DO PROF. REGULAÇÃO', 
                        'FARMÁCIA', 'TURNO DO PROF. FARMÁCIA', 'HORÁRIO DO PROF. FARMÁCIA']
    
    df_usf_mais = df_usf_mais['USF+']
    df_usf_mais = df_usf_mais[usf_mais_columns]
    df_usf_mais = remove_espacos_e_acentos(df_usf_mais)
    df_usf_mais = add_pk(df_usf_mais, 'equipes_usf_mais')

    # df_gerentes 
    url_gerentes = '/Shared Documents/SESAU/NGI/outros/CONTATOS_GERENTES_DE_UNIDADES_GGGD.xlsx'
    df_gerentes = get_file_as_dataframes(url_gerentes, skiprows= 1)


    gerentes_columns = ['DS', 'CNES', 'UNIDADE', 'GERENTE','CARGO', 'CPF', 'MATRÍCULA', 'CONTATO', 'E-MAIL']

    df_gerentes = df_gerentes['atencao_basica']
    df_gerentes = df_gerentes[gerentes_columns]
    df_gerentes = remove_espacos_e_acentos(df_gerentes)
    df_gerentes = add_pk(df_gerentes, 'gerentes')


    return {
        'servidores': df_servidores,
        'equipes': df_equipes,
        'equipes_usf_mais': df_usf_mais,
        'gerentes': df_gerentes
    }