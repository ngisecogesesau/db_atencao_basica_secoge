
# esta no google drive porque atualiza direto. da pra pegar tambem no dw de homero




# import pandas as pd
# import logging

# from src.utils.extract_sharepoint_df import get_file_as_dataframes
# from src.utils.excel_operations import remove_espacos_e_acentos
# from src.utils.add_primary_key import add_pk

# def read_agendamentos():
#     """
#     Ler e processar dados para a tabela de 'agendamentos' a partir da aba 'Planilha1'.

#     :return: Um DataFrame processado para 'agendamentos'
#     """
#     relative_url_agendamentos = "/Shared Documents/SESAU/BI_Indicadores_Estrategicos/agendamentos.xlsx"

#     df_dict = get_file_as_dataframes(relative_url_agendamentos)

#     if 'Planilha1' not in df_dict:
#         logging.error("Aba 'Planilha1' não encontrada no arquivo Excel.")
#         return pd.DataFrame()
    
#     df_agendamentos = df_dict['Planilha1']

#     required_columns = [
#         'co_unidade_saude', 'no_unidade_saude', 'nu_cnes', 'co_equipe', 'nu_ine', 'no_equipe', 
#         'no_profissional', 'dt_agendado', 'hr_inicial_agendado', 'no_cidadao', 'nu_cpf', 'nu_cns', 
#         'nu_telefone_residencial', 'nu_telefone_celular', 'nu_telefone_contato', 'ds_observacao', 
#         'st_agendado', 'no_situacao_agendado', 'dt_criacao', 'co_seq_prontuario', 'qt_referencia', 
#         'co_prontuario_grupo', 'co_cidadao', 'st_cidadao_processado', 'co_cbo', 'chave', 'esp_ini', 
#         'esp_fim', 'esp_codigo', 'dia_semana', 'qtd_atendimentos', 'turno', 'atendimento', 'expansão', 
#         'ESF+', 'Mês', 'Ano', 'Mês Criação', 'Ano Criação', 'dt_nascimento', 'Idade', 'Faixa etária', 
#         'co_escolaridade', 'no_escolaridade', 'no_sexo', 'Semana_do_Ano', 'Unidades.DISTRITO', 'co_prof', 
#         'Cancelamento Reagendado?', 'ds'
#     ]
        
#     df_agendamentos = df_agendamentos[required_columns]
#     df_agendamentos = remove_espacos_e_acentos(df_agendamentos)
#     df_agendamentos = add_pk(df_agendamentos, 'agendamentos')

#     return {
#         'agendamentos': df_agendamentos,
#     }
