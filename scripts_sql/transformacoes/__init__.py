from .profissionais_equipes_duckdb import (
    create_servidores_table, create_equipes_table, create_tipo_equipe_table,
    update_equipes_table, update_servidores_table, create_equipes_usf_mais_table,
    create_gerentes_table, update_gerentes_table, update_equipes_usf_mais_table
)
from .unidades_duckdb import (
    create_unidades_table, create_tipo_unidade_table, create_horarios_table,
    create_distritos_table, update_unidades_table, create_login_senha_ds_table,
    create_login_senha_unidades_table
)
from .asu_duckdb import (
    create_asu_classificacao_table, create_asu_monitora_table,
    create_equipes_asu_table, create_unidades_equipes_asu,
    update_asu_monitora_table, update_equipes_asu_relacionamento_equipes,
    update_equipes_asu_relacionamento_unidades, update_unidades_quipes_asu_relacionamento_unidades
)
from .agendamentos_duckdb import (
    create_bd_agendamentos_table, update_bd_agendamentos_table, create_bd_agenda_configurada, 
    create_interdicoes
)
from .atendimentos_duckdb import create_atendimentos_table, update_atendimentos_table, create_relacionamento_atendimentos_calendario
from .calendario_duckdb import create_calendario_table
from .ouvidoria_duckdb import create_ouvidoria_table
from .coletas_duckdb import create_coletas_postos_table, create_dados_qualidade_coleta_laboratorio_clinica
from .previne_duckdb import create_serie_historica_previne, create_resultado_indicadores_desempenho_consolidado_ms
from .sevs_duckdb import create_areas_cobertas_psa_table
from .gratificacoes_duckdb import create_gratificacoes_unidades

def execute_transformations_and_save(con, engine):
    transformations = [
        (create_calendario_table, 'calendario', 'calendario'),
        
        (create_unidades_table, 'unidades', 'unidades'),
        (create_tipo_unidade_table, 'tab_tipo_unidade', 'unidades'),
        (create_horarios_table, 'horarios', 'unidades'),
        (create_distritos_table, 'distritos', 'unidades'),
        (update_unidades_table, 'unidades', 'unidades'),
        (create_login_senha_ds_table, 'login_senha_ds', 'unidades'),
        (create_login_senha_unidades_table, 'login_senha_ds_unidades', 'unidades'),

        (create_servidores_table, 'servidores', 'profissionais_equipes'),
        (update_servidores_table, 'servidores', 'profissionais_equipes'),
        (create_equipes_table, 'equipes', 'profissionais_equipes'),
        (update_equipes_table, 'equipes', 'profissionais_equipes'),
        (create_tipo_equipe_table, 'tipo_equipe', 'profissionais_equipes'),
        (create_equipes_usf_mais_table, 'equipes_usf_mais', 'profissionais_equipes'),
        (update_equipes_usf_mais_table, 'equipes_usf_mais', 'profissionais_equipes'),
        (create_gerentes_table, 'gerentes', 'profissionais_equipes'),
        (update_gerentes_table, 'gerentes', 'profissionais_equipes'),

        (create_asu_monitora_table, 'asu_monitora', 'asu'),
        (update_asu_monitora_table, 'asu_monitora', 'asu'),
        (create_asu_classificacao_table, 'asu_classificacao', 'asu'),
        (create_equipes_asu_table, 'equipes_asu', 'asu'),
        (update_equipes_asu_relacionamento_equipes, 'equipes_asu', 'asu'),
        (update_equipes_asu_relacionamento_unidades, 'equipes_asu', 'asu'),
        (create_unidades_equipes_asu, 'unidades_equipes_asu', 'asu'),
        (update_unidades_quipes_asu_relacionamento_unidades, 'unidades_equipes_asu', 'asu'),

        (create_bd_agendamentos_table, 'bd_agendamentos', 'agendamentos'),
        (update_bd_agendamentos_table, 'bd_agendamentos', 'agendamentos'),
        (create_bd_agenda_configurada, 'bd_agenda_configurada', 'agendamentos'),
        (create_interdicoes, 'interdicoes', 'agendamentos'),

        (create_atendimentos_table, 'atendimentos', 'atendimentos'),
        (update_atendimentos_table, 'atendimentos', 'atendimentos'),
        (create_relacionamento_atendimentos_calendario,  'atendimentos', 'atendimentos'),

        (create_ouvidoria_table, 'ouvidoria', 'ouvidoria'),

        (create_coletas_postos_table, 'coletas_postos', 'coletas'),
        (create_dados_qualidade_coleta_laboratorio_clinica, 'dados_qualidade_coleta_laboratorio_clinica', 'coletas'),
        (create_serie_historica_previne, 'serie_historica_previne', 'previne'),
        (create_resultado_indicadores_desempenho_consolidado_ms, 'resultado_indicadores_desempenho_consolidado_ms', 'previne'),
        (create_areas_cobertas_psa_table, 'areas_cobertas_psa', 'sevs'),
        (create_gratificacoes_unidades, 'gratificacoes_unidades', 'gratificacoes')
    ]

    for transformation_func, table_name, schema_name in transformations:
        df = transformation_func(con)
        save_table_to_postgres(df, table_name, engine, schema_name)

def save_table_to_postgres(df, table_name, engine, schema_name):
    import logging
    logger = logging.getLogger(__name__)
    try:
        df.to_sql(table_name, engine, schema=schema_name, if_exists='replace', index=False)
        logger.info(f"Tabela '{table_name}' salva no esquema '{schema_name}' do banco de dados PostgreSQL com sucesso.")
    except Exception as e:
        logger.error(f"Erro ao salvar tabela '{table_name}': {e}")
