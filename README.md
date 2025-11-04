# Data Warehouse - Credits Brasil

## 1. Visão Geral

Esta solução de Data Warehouse foi projetada para consolidar dados de múltiplas fontes em uma camada Bronze em um banco de dados PostgreSQL. O objetivo principal é criar uma fonte única de verdade para dados brutos, que podem ser usados para análises e relatórios.

## 2. Arquitetura

A arquitetura segue um modelo de camada única (Bronze) onde os dados são ingeridos de fontes externas e armazenados em sua forma bruta, com o mínimo de transformação.

- **Fontes de Dados:** Arquivos CSV
- **Camada de Destino:** Bronze (Raw Data)
- **Banco de Dados:** PostgreSQL

## 3. Pré-requisitos

Antes de iniciar, certifique-se de que os seguintes softwares estão instalados em sua máquina:

- Docker (versão 20 ou superior)
- Docker Compose
- Python (versão 3.10 ou superior)
- Um cliente PostgreSQL (como `psql` ou DBeaver) para interagir com o banco de dados.

## 4. Instalação e Configuração

Siga os passos abaixo para configurar o ambiente de desenvolvimento.

### 4.1. Clonar o Repositório

```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

### 4.2. Configurar Variáveis de Ambiente

Crie um arquivo chamado `.env` na raiz do projeto. Este arquivo armazenará as credenciais de conexão com o banco de dados PostgreSQL. Adicione as seguintes variáveis ao arquivo `.env` e substitua pelos seus valores:

```
DB_HOST=seu_host_de_banco_de_dados
DB_PORT=sua_porta
DB_NAME=seu_nome_de_banco_de_dados
DB_USER=seu_usuario
DB_PASSWORD=sua_senha
```

### 4.3. Inicializar o Banco de Dados

Os scripts SQL para criar os schemas e as tabelas necessárias estão localizados no diretório `sql/`. Você precisa executá-los no seu banco de dados PostgreSQL.

```bash
# Conecte-se ao seu banco de dados e execute os seguintes comandos:
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sql/init/01-create-schemas.sql
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sql/init/02-create-audit-table.sql
psql -h $DB_HOST -p $DB_PORT -U $DB_USER -d $DB_NAME -f sql/bronze/01-create-bronze-tables.sql
```

## 5. Ingestão de Dados

O processo de ingestão de dados é executado através de scripts Python orquestrados pelo Docker Compose.

### 5.1. Preparar os Arquivos de Dados

Coloque os arquivos CSV que você deseja ingerir no diretório `docker/data/input/onedrive/`. O nome do arquivo deve corresponder ao nome esperado pelo script de ingestão (ex: `contas_base_oficial.csv`).

### 5.2. Executar o Container Docker

Navegue até o diretório `docker` e inicie o container do processador de ETL em modo detached:

```bash
cd docker
docker compose up -d
```

### 5.3. Executar um Script de Ingestão

Use o comando `docker compose exec` para executar um script de ingestão específico. Por exemplo, para ingerir o arquivo `contas_base_oficial.csv`:

```bash
docker compose exec etl-processor python python/ingestors/csv/ingest_contas_base_oficial.py
```

## 6. Desenvolvimento

### 6.1. Qualidade de Código

O projeto utiliza as seguintes ferramentas para garantir a qualidade do código:

- **Formatação:** `black`
- **Linting:** `flake8`
- **Type Checking:** `mypy`

Para executar as ferramentas, use os seguintes comandos:

```bash
black python/
flake8 python/
mypy python/
```

## 7. Estrutura do Banco de Dados

A camada Bronze contém as seguintes tabelas:

- **`bronze.contas_base_oficial`**: Armazena os dados da base oficial de contas.
- **`bronze.faturamento`**: Armazena os dados de faturamento.
- **`bronze.data`**: Tabela de dados genéricos.
- **`bronze.usuarios`**: Armazena informações de usuários.
- **`credits.historico_atualizacoes`**: Tabela de auditoria que registra todas as execuções de ingestão.

## 8. Segurança

- O arquivo `.env` contém informações sensíveis e **nunca** deve ser commitado no repositório. Ele já está incluído no `.gitignore` para prevenir commits acidentais.
- Recomenda-se o uso de roles e permissões específicas no PostgreSQL para limitar o acesso do usuário da aplicação.

## 9. Suporte

Para relatar problemas ou solicitar suporte, por favor, abra uma issue no repositório do GitHub.

## 10. Licença

Este projeto é de propriedade da Credits Brasil © 2025.
