# DB Atbasica - ETL para Painéis Power BI

## Descrição

Este repositório contém um projeto de ETL (Extração, Transformação e Carga) desenvolvido para automatizar o processo de coleta, transformação e carga de dados da Atenção Básica em Saúde. O objetivo é alimentar painéis de análise no Power BI com dados transformados e centralizados em um banco de dados PostgreSQL. O pipeline é construído em Python e utiliza diversas bibliotecas e tecnologias para facilitar a extração de dados de fontes múltiplas, transformação dos dados para atender aos requisitos de visualização e carga no banco de dados.

## Funcionalidades

- **Extração**: Coleta de dados a partir de diversas fontes, incluindo Google Sheets, SharePoint e arquivos Excel.
- **Transformação**: Processamento e transformação dos dados para adequação ao modelo de dados, com integração e normalização das tabelas.
- **Carga**: Inserção dos dados transformados em um banco de dados PostgreSQL, estruturado em diferentes schemas para organização dos dados.
- **Integração com Power BI**: Os dados processados são disponibilizados para atualização direta dos painéis do Power BI.

## Estrutura do Projeto

A estrutura de pastas do projeto é organizada conforme descrito abaixo:

- **`src`**: Contém o código principal de processamento de dados.
  - **`data_processing`**: Scripts para processamento e transformação de dados de diferentes fontes, como `asu.py`, `atendimentos.py`, etc.
  - **`database`**: Contém o script para criar a conexão com o banco de dados PostgreSQL.
  - **`sql_operations`**: Scripts para operações SQL específicas, como criação de schemas.
  - **`utils`**: Utilitários para tarefas comuns, como manipulação de planilhas Excel, autenticação com SharePoint e Google Sheets, e manipulação de chaves primárias.

- **`scripts_main`**: Scripts principais para execução do ETL.
  - **`main.py`**: Script principal para execução do pipeline ETL.
  
- **`scripts_sql/transformacoes`**: Scripts que contêm as transformações específicas para cada tabela, utilizando DuckDB como ambiente intermediário.

## Tecnologias Utilizadas

- **Python**: Linguagem de programação utilizada para implementar o pipeline ETL.
- **PostgreSQL**: Banco de dados utilizado para armazenar os dados transformados.
- **DuckDB**: Utilizado para manipulação intermediária dos dados durante as transformações.
- **Poetry**: Gerenciador de dependências e ambientes para Python.
- **pyenv**: Utilizado para gerenciar diferentes versões do Python.
- **Google Sheets API** e **SharePoint API**: APIs utilizadas para extração de dados de fontes online.

## Pré-requisitos

Antes de executar o projeto, certifique-se de que você tem os seguintes itens instalados:

- **Python 3.12** ou superior (gerenciado via **pyenv**)
- **PostgreSQL**
- **Poetry**

## Instalação

1. **Clone o repositório**:

   ```bash
   git clone https://github.com/seu-usuario/db-atbasica.git
   cd db-atbasica
