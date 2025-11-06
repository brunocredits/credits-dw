# 04 - Estrutura e Arquitetura do Projeto

Este documento fornece uma visão geral da arquitetura de dados e da organização das pastas e arquivos no projeto do Data Warehouse.

---

## 1. Arquitetura de Dados

O projeto segue uma arquitetura de **Camada Bronze (Bronze Layer)**, um conceito fundamental em Data Lakes e Data Warehouses modernos.

*   **O que é?** A Camada Bronze é a primeira parada para os dados brutos que vêm das fontes externas (neste caso, os arquivos CSV).
*   **Objetivo:** Armazenar os dados em seu estado original, com o mínimo de transformação possível. As únicas alterações feitas são geralmente para padronizar o formato (ex: renomear colunas para nomes válidos, converter tudo para texto).
*   **Vantagens:**
    *   **Fonte da Verdade:** A Camada Bronze serve como uma "fonte única da verdade" para os dados brutos. Se algo der errado nas camadas futuras (Silver, Gold), sempre podemos reconstruir os dados a partir da Bronze.
    *   **Rastreabilidade:** Facilita a auditoria e a depuração, pois podemos comparar os dados processados com os dados originais.

O fluxo de dados atual é:

`Arquivos CSV → Scripts de Ingestão (Python) → Camada Bronze (PostgreSQL)`

---

## 2. Estrutura de Pastas do Projeto

A organização das pastas foi pensada para separar as responsabilidades:

```
credits.dw/
├── docs/                  # Documentação do projeto (onde você está agora)
├── docker/                # Arquivos de configuração do Docker
│   ├── Dockerfile         # Define a imagem Python para o ETL
│   ├── docker-compose.yml # Orquestra os serviços (contêiner de ETL)
│   └── data/              # Dados de entrada, processados e templates
│       ├── input/         # Arquivos CSV a serem processados
│       ├── processed/     # Backup dos arquivos que já foram processados
│       └── templates/     # Modelos de arquivos CSV com cabeçalhos
├── python/                # Código-fonte dos scripts de ETL
│   ├── ingestors/         # Scripts que fazem a ingestão dos dados
│   └── utils/             # Módulos de utilidade (conexão, log, etc.)
├── .env                   # Arquivo de configuração local (NÃO VERSIONADO)
├── .gitignore             # Arquivos e pastas a serem ignorados pelo Git
└── requirements.txt       # Dependências Python do projeto
```

---

## 3. Schemas do Banco de Dados

O banco de dados está organizado em dois schemas principais:

### `bronze`

*   **Propósito:** Contém as tabelas com os dados brutos ingeridos dos arquivos CSV.
*   **Tabelas Principais:** `contas_base_oficial`, `faturamento`, `usuarios`, `data`.
*   **Característica:** Os dados aqui são uma cópia fiel das fontes, com o mínimo de processamento.

### `credits`

*   **Propósito:** Schema de metadados, usado para controle e auditoria do próprio processo de ETL.
*   **Tabelas Principais:**
    *   `historico_atualizacoes`: Registra cada execução dos scripts de ingestão, incluindo o nome do script, a data/hora, o status (sucesso/erro), e a quantidade de linhas processadas. É fundamental para monitorar a saúde do pipeline.

---

Esta estrutura modular permite que o projeto cresça de forma organizada, facilitando a adição de novas fontes de dados, novas camadas de processamento (Silver, Gold) e a manutenção do código existente.
