# 🏦 Data Warehouse Credits Brasil

> **Versão:** 2.0 | **Arquitetura:** Bronze Layer | **PostgreSQL** 15

---

## 📋 Visão Geral

Esta é uma solução de Data Warehouse para consolidar dados de diversas fontes em um banco de dados PostgreSQL. O projeto utiliza um pipeline de ETL (Extração, Transformação e Carga) para processar arquivos CSV e carregá-los em uma camada **Bronze**, garantindo que os dados brutos sejam armazenados com o mínimo de transformação.

O ambiente é totalmente orquestrado com Docker, garantindo consistência e facilidade de uso.

### ✨ Recursos Principais

- ✅ **Camada Bronze**: Armazena dados brutos de fontes CSV.
- ✅ **Scripts de Ingestão em Python**: Para um ETL robusto e modular.
- ✅ **Orquestração com Docker**: Ambiente de desenvolvimento e produção consistente.
- ✅ **Auditoria de Execução**: Rastreia o status de cada ingestão no schema `credits`.

---

## 🏗️ Arquitetura de Dados

A arquitetura de dados é focada na simplicidade e robustez, com uma clara separação de responsabilidades.

```
FONTES (Arquivos CSV) → CAMADA BRONZE (Dados Brutos)
```

### Schemas do Banco de Dados

-   **`bronze`**: Contém os dados brutos exatamente como vêm das fontes, com o mínimo de processamento (ex: renomear colunas). É a nossa fonte única da verdade para os dados originais.
-   **`credits`**: Schema de metadados, usado para auditoria e controle do próprio processo de ETL. A tabela `historico_atualizacoes` registra cada execução dos scripts, seu status, duração e volume de dados.

### A Tabela de Dimensão de Data (`bronze.data`)

Um destaque da nossa modelagem é a tabela `bronze.data`. Embora pareça redundante à primeira vista, ela é uma ferramenta poderosa de análise conhecida como **Tabela de Dimensão de Data**.

-   **Como funciona?** Para cada dia do calendário, armazenamos a data completa (`data_completa`) e também vários atributos pré-calculados como `semestre`, `trimestre`, `mes` e `ano`.
-   **Por que usar?**
    1.  **Performance:** Consultas que agregam dados por períodos (ex: receita por trimestre) se tornam extremamente rápidas, pois o banco de dados não precisa calcular a qual trimestre uma data pertence para milhões de registros; ele simplesmente usa o valor que já está armazenado.
    2.  **Simplicidade:** As consultas SQL ficam mais limpas e fáceis de ler (`GROUP BY semestre` em vez de usar funções de data complexas).
    3.  **Consistência:** Garante que todos na empresa usem a mesma definição para períodos de tempo, evitando inconsistências em relatórios.

---

## 📂 Estrutura do Projeto

```
credits-dw/
├── docker/
│   ├── Dockerfile             # Define a imagem do container de ETL
│   ├── docker-compose.yml     # Orquestra os serviços
│   └── data/
│       ├── templates/         # Contém arquivos CSV de EXEMPLO com cabeçalhos
│       └── input/             # Onde os arquivos a serem processados devem estar
│           └── onedrive/
├── python/
│   ├── ingestors/             # Scripts de ingestão por fonte (ex: csv)
│   └── utils/                 # Módulos de utilidade (conexão, log, etc.)
├── .env                       # Arquivo de configuração local (NÃO VERSIONADO)
├── README.md                  # Esta documentação
└── requirements.txt           # Dependências Python
```

---

## 🚀 Instalação e Uso

### Pré-requisitos

-   Docker e Docker Compose V2 (comando `docker compose`)
-   Python 3.10+ (para desenvolvimento local)
-   Um cliente PostgreSQL (ex: DBeaver, pgAdmin) para se conectar ao banco.

### 1. Configurar o Ambiente

1.  **Clone o repositório:**
    ```bash
    git clone <URL_DO_REPOSITORIO>
    cd credits-dw
    ```

2.  **Crie o arquivo de ambiente:**
    Na raiz do projeto, crie um arquivo chamado `.env`. Ele guardará suas credenciais de banco de dados de forma segura. Copie o conteúdo abaixo e preencha com seus dados.

    ```properties
    # Credenciais do Banco de Dados PostgreSQL
    DB_HOST=<seu_host>
    DB_PORT=<sua_porta>
    DB_NAME=<seu_banco>
    DB_USER=<seu_usuario>
    DB_PASSWORD=<sua_senha>
    ```
    > **Segurança:** O arquivo `.env` já está no `.gitignore`, garantindo que suas credenciais nunca sejam enviadas para o repositório.

### 2. Preparar os Dados de Exemplo

Os scripts de ingestão procuram por arquivos CSV no diretório `docker/data/input/onedrive/`. Para testar o pipeline, copie os arquivos de exemplo do diretório `templates`:

```bash
# Copia os arquivos de exemplo para o local de ingestão
cp docker/data/templates/*.csv docker/data/input/onedrive/
```

### 3. Iniciar o Ambiente Docker

Todos os comandos devem ser executados a partir da raiz do projeto.

1.  **Construir e iniciar o container de ETL:**
    Este comando iniciará o serviço `etl-processor` em segundo plano. O container ficará ativo, pronto para executar os scripts.

    ```bash
    # Navegue até o diretório docker e suba o container
    cd docker && docker compose up -d --build
    ```

2.  **Executar os Scripts de Ingestão:**
    Para executar um script, use o comando `docker compose exec`.

    ```bash
    # Para executar TODOS os ingestores de CSV de uma vez
    docker compose exec etl-processor python python/run_all_ingestors.py

    # Para executar um ingestor específico (ex: faturamento)
    docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py
    ```

3.  **Parar o Ambiente:**
    Quando terminar, você pode parar e remover os containers.

    ```bash
    docker compose down
    ```

---

## 🛠️ Desenvolvimento

### Acessando o Container

Para depurar ou executar comandos manualmente dentro do container:

```bash
docker compose exec etl-processor bash
```

### Qualidade de Código

O projeto usa as seguintes ferramentas para garantir a qualidade do código:

```bash
# Formatação
black python/

# Linting
ruff check .

# Checagem de tipos
mypy python/
```
