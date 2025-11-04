# Data Warehouse - Credits Brasil

## 1. Visão Geral

Esta solução de Data Warehouse foi projetada para consolidar dados de múltiplas fontes em uma camada Bronze em um banco de dados PostgreSQL. O objetivo principal é criar uma fonte única de verdade para dados brutos, que podem ser usados para análises e relatórios.

## 2. Arquitetura

A arquitetura segue um modelo de camada única (Bronze) onde os dados são ingeridos de fontes externas e armazenados em sua forma bruta, com o mínimo de transformação.

- **Fontes de Dados:** Arquivos CSV
- **Camada de Destino:** Bronze (Raw Data)
- **Banco de Dados:** PostgreSQL (Azure)

## 3. Pré-requisitos

Antes de iniciar, certifique-se de que os seguintes softwares estão instalados em sua máquina:

- Docker (versão 20 ou superior)
- Docker Compose
- Python (versão 3.10 ou superior)

## 4. Instalação e Configuração

Siga os passos abaixo para configurar o ambiente de desenvolvimento.

### 4.1. Clonar o Repositório

```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

### 4.2. Configurar o Acesso ao Banco de Dados

As credenciais de acesso ao banco de dados PostgreSQL no Azure estão hardcoded no arquivo `python/utils/db_connection.py`. Certifique-se de que o seu endereço de IP está liberado no firewall do Azure para permitir conexões ao banco de dados.

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

## 8. Suporte

Para relatar problemas ou solicitar suporte, por favor, abra uma issue no repositório do GitHub.

## 9. Licença

Este projeto é de propriedade da Credits Brasil © 2025.