# Modelagem dos dados

## Introdução

## Schemas

### SCHEMA unidades

#### Descrição Geral:

Armazena as informações principais das unidades de saúde, identificadas por um código único (cnes_padrao). Inclui dados como nome, distrito, unidade e coordenadas geográficas.

#### Tabelas:

Tabela **unidades:** Armazena as informações principais das unidades de saúde, identificadas por um código único (cnes_padrao). Inclui dados como nome, distrito, unidade, coordenadas geográficas, e chaves estrangeiras que referenciam outras tabelas.

- **cnes:** Identificador único da unidade de saúde.
- **nome:** Nome da unidade.
- **distrito:** Distrito onde a unidade está localizada.
- **unidade:** Nome da unidade específica.
- **cod_unidade:** Código específico da unidade.
- **x_long:** Coordenada de longitude da unidade.
- **y_lat:** Coordenada de latitude da unidade.
- **fk_id_tipo_unidade:** Chave estrangeira para a tabela tipoUnidade.
- **fk_id_horarios:** Chave estrangeira para a tabela Horarios.
- **fk_id_info_unidades:** Chave estrangeira para a tabela info_unidades.
- **fk_id_distritos:** Chave estrangeira para a tabela distritos.


Tabela **tipoUnidade:** Descreve os diferentes tipos de unidades, proporcionando uma camada de descrição adicional.

- **id_tipo_unidade:** Identificador único do tipo de unidade.
- **tipo_unidade:** Tipo da unidade (e.g., USF, USF+).
- **descricao:** Descrição adicional do tipo de unidade.
- **fk_cnes_padrao:** Chave estrangeira para a tabela unidades.

Tabela **horarios:** Armazena os horários de funcionamento das unidades, especificando o tipo de horário.

- **id_horarios:** Identificador único do horário.
- **turno:** Turno (Matutino, Vespertino, Noturno ou Integral).
- **horario_inicio:** Hora de início do expediente.
- **horario_fim:** Hora de término do expediente.
- **fk_id_unidade:** Chave estrangeira para a tabela unidades.

Tabela **info_unidades:** Contém características específicas das unidades, como quantidades de profissionais e indicadores booleanos.

- **id_info_unidades:** Identificador único das informações da unidade.
- **cnes_padrao:** Identificador único da unidade de saúde.
- **qtd_recepcionista:** Quantidade de recepcionistas.
- **qtd_farmacia:** Quantidade de farmácias.
- **regulacao:** Indicador booleano de regulação.
- **acs:** Indicador booleano de presença de agentes comunitários de saúde.
- **n_esb:** Número de equipes da saúde bucal.
- **cir_dentista:** Indicador booleano de presença de cirurgião dentista.
- **asb:** Indicador booleano de presença de assistente de saúde bucal.
- **recepcionista:** Indicador booleano de presença de recepcionista.
- **fk_cnes_padrao:** Chave estrangeira para a tabela Unidades.


![Clique aqui para acessar o diagrama do Schema Unidades](https://raw.githubusercontent.com/ngisecogesesau/db_atencao_basica_secoge/blob/main/modelagem/schema_unidades.drawio.png)

### Schema funcionarios_equipes

#### Descrição Geral:

Armazena as informações principais dos funcionários e equipes das unidades de saúde. Este schema inclui dados detalhados sobre servidores, tipos de equipe, equipes e gerentes de unidades.

Tabela **servidor**

- **id_servidor (PK):** Identificador único do servidor.
- **nome_servidor:** Nome do servidor.
- **situacao_funcional:** Situação funcional do servidor (ex.: ATIVO).
- **perfil_cargo:** Perfil do cargo do servidor (ex.: ACS).
- **cod_und_lotacao:** Código da unidade de lotação.
- **perfil_und_lotacao:** Perfil da unidade de lotação (ex.: tipo_unidade).
- **cnes_und_lotacao:** Código CNES da unidade de lotação.
- **setor:** Setor de trabalho do servidor.
- **turno_trabalho:** Turno de trabalho do servidor.
- **fk_id_unidades (FK):** Chave estrangeira que referencia a tabela unidade no schema unidades.
- **fk_id_distritos (FK):** Chave estrangeira que referencia a tabela tab_distritos no schema unidades.

Tabela **tipoEquipe:**

- **id_tipo_equipe (PK):** Identificador único do tipo de equipe.
- **tipo_equipe:** Descrição do tipo de equipe.

Tabela **equipes:**

- **id_equipe_cnes (PK):** Identificador único da equipe CNES.
- **seq_equipe:** Sequência única da equipe.
- **descricao_equipe:** Descrição da equipe.
- **nome_equipe:** Nome da equipe.
- **turno:** Turno de atendimento da equipe.
- **criacao_equipe:** Data de criação da equipe.
- **data_desativacao:** Data de desativação da equipe.
- **sigla_equipe:** Sigla da equipe.
- **fk_id_tipo_equipe (FK):** Chave estrangeira que referencia a tabela tipoEquipe.
- **fk_id_unidades (FK):** Chave estrangeira que referencia a tabela unidade no schema unidades.

Tabela **gerente:**

- **id_gerente (PK):** Identificador único do gerente.
- **distrito:** Distrito do gerente.
- **nome_unidade:** Nome da unidade de saúde gerenciada.
- **nome_gerente:** Nome do gerente.
- **cargo:** Cargo do gerente (ex.: gerente de unidade de saúde).
- **cpf:** CPF do gerente (com ponto e traço).
- **contato:** Telefone de contato do gerente.
- **email:** Email de contato do gerente.
- **fk_id_unidades (FK):** Chave estrangeira que referencia a tabela unidade no schema unidades.
- **fk_id_distritos (FK):** Chave estrangeira que referencia a tabela tab_distritos no schema unidades.

**Relacionamentos com o Schema unidades**

As tabelas do schema **funcionarios_equipes** estão relacionadas às tabelas do schema **unidades** da seguinte forma:

Tabela servidor:

- fk_id_unidades referencia unidade no schema unidades.
- fk_id_distritos referencia tab_distritos no schema unidades.

Tabela equipes:

- fk_id_unidades referencia unidade no schema unidades.

Tabela gerente:

- fk_id_unidades referencia unidade no schema unidades.
- fk_id_distritos referencia tab_distritos no schema unidades.

![Clique aqui para acessar o diagrama do Schema funcionarios_equipes](https://raw.githubusercontent.com/ngisecogesesau/db_atencao_basica_secoge/blob/main/modelagem/schema_funcionarios_equipes.drawio)