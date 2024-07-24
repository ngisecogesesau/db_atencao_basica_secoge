# Modelagem dos dados

## Introdução

## Schemas

### Schema Unidades

#### Descrição Geral:

Armazena as informações principais das unidades de saúde, identificadas por um código único (cnes_padrao). Inclui dados como nome, distrito, unidade, coordenadas geográficas, e chaves estrangeiras que referenciam outras tabelas.

#### Tabelas:

**Tabela unidades:** Armazena as informações principais das unidades de saúde, identificadas por um código único (cnes_padrao). Inclui dados como nome, distrito, unidade, coordenadas geográficas, e chaves estrangeiras que referenciam outras tabelas.

- **cnes_padrao:** Identificador único da unidade de saúde.
- **nome:** Nome da unidade.
- **distrito:** Distrito onde a unidade está localizada.
- **unidade:** Nome da unidade específica.
- **cod_unidade:** Código específico da unidade.
- **x_long:** Coordenada de longitude da unidade.
- **y_lat:** Coordenada de latitude da unidade.
- **fk_id_tipo_unidade:** Chave estrangeira para a tabela TipoUnidade.
- **fk_id_horarios:** Chave estrangeira para a tabela Horarios.
- **fk_id_info_unidades:** Chave estrangeira para a tabela Info_unidades.


**Tabela tipoUnidade:** Descreve os diferentes tipos de unidades, proporcionando uma camada de descrição adicional.

- **id_tipo_unidade:** Identificador único do tipo de unidade.
- **tipo_unidade:** Tipo da unidade (e.g., USF, USF+).
- **descricao:** Descrição adicional do tipo de unidade.
- **fk_cnes_padrao:** Chave estrangeira para a tabela unidades.

**Tabela horarios:** Armazena os horários de funcionamento das unidades, especificando o tipo de horário.

- **id_horarios:** Identificador único do horário.
- **tipo_horario:** Tipo do horário (e.g., ATENDIMENTO, RECEPCAO).
- **horario_inicio:** Hora de início do horário.
- **horario_fim:** Hora de término do horário.
- **fk_cnes_padrao:** Chave estrangeira para a tabela Unidades.

**Tabela Info_unidades:** Contém características específicas das unidades, como quantidades de profissionais e indicadores booleanos.

**id_info_unidades:** Identificador único das informações da unidade.
**cnes_padrao:** Identificador único da unidade de saúde.
**qtd_recepcionista:** Quantidade de recepcionistas.
**qtd_farmacia:** Quantidade de farmácias.
**regulacao:** Indicador booleano de regulação.
**acs:** Indicador booleano de presença de agentes comunitários de saúde.
**n_esb:** Número de equipes da saúde bucal.
**cir_dentista:** Indicador booleano de presença de cirurgião dentista.
**asb:** Indicador booleano de presença de assistente de saúde bucal.
**recepcionista:** Indicador booleano de presença de recepcionista.
**fk_cnes_padrao:** Chave estrangeira para a tabela Unidades.

![Diagrama de Exemplo](https://github.com/ngisecogesesau/db_atencao_basica_secoge/tree/main/modelagem/schema_unidades.drawio.png)


## TABELAS ALTERADAS

### SCHEMA profissionais_equipes

EQUIPES_CNES -> equipes
USF -> servidores