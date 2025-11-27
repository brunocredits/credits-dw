# GUIA TÉCNICO COMPLETO - Data Warehouse Credits Brasil

**Data:** 27/11/2025
**Versão:** 5.0
**Autor:** Equipe Engenharia de Dados
**Status:** ✅ Produção

---

## ÍNDICE

1. [Visão Geral do Sistema](#1-visão-geral-do-sistema)
2. [Arquitetura Completa](#2-arquitetura-completa)
3. [Configuração e Variáveis de Ambiente](#3-configuração-e-variáveis-de-ambiente)
4. [Camada Bronze - Ingestão e Validação](#4-camada-bronze---ingestão-e-validação)
5. [Camada Silver - Star Schema](#5-camada-silver---star-schema)
6. [Camada Gold - Agregações Analíticas](#6-camada-gold---agregações-analíticas)
7. [Fluxo de Dados Completo com Exemplos Reais](#7-fluxo-de-dados-completo-com-exemplos-reais)
8. [Classes e Métodos Principais](#8-classes-e-métodos-principais)
9. [Sistema de Auditoria e Logs](#9-sistema-de-auditoria-e-logs)
10. [Troubleshooting e FAQ](#10-troubleshooting-e-faq)

---

## 1. VISÃO GERAL DO SISTEMA

### 1.1 O que é este projeto?

Este é um **Data Warehouse (DW)** completo para a Credits Brasil, implementando uma **arquitetura Medallion** (Bronze → Silver → Gold) para consolidar e analisar dados financeiros de:

- 📊 **Faturamento**: Receitas, moedas, valores
- 👥 **Clientes**: Empresas (PJ) e pessoas (PF)
- 🧑‍💼 **Usuários/Consultores**: Vendedores e suas hierarquias
- 📅 **Datas**: Calendário dimensional com atributos de negócio

### 1.2 Dados Atuais (27/11/2025)

```
CAMADA BRONZE (Dados Brutos Validados):
├─ bronze.contas         → 10 registros
├─ bronze.usuarios       → 12 registros
├─ bronze.faturamentos   → 13 registros
└─ bronze.data          → 366 registros (calendário 2024)
   TOTAL: 401 registros validados

CAMADA SILVER (Star Schema):
├─ Dimensões:
│  ├─ dim_cliente        → 10 registros (SCD Type 2)
│  ├─ dim_usuario        → 12 registros (SCD Type 2)
│  └─ dim_data           → 319 registros
├─ Fatos:
│  └─ fato_faturamento   → 13 registros
   TOTAL: 354 registros

CAMADA GOLD (Views Agregadas):
├─ vendas_diarias        → 13 registros
├─ vendas_semanais       → 13 registros
├─ vendas_mensais        → 12 registros
├─ carteira_clientes     → 13 registros
└─ performance_consultores → 12 registros
   TOTAL: 63 registros agregados
```

### 1.3 Tecnologias Utilizadas

| Componente | Tecnologia | Versão | Propósito |
|------------|------------|--------|-----------|
| **Database** | PostgreSQL | 15 | Armazenamento e processamento |
| **Hosting** | Azure Database for PostgreSQL | - | Managed database na nuvem |
| **Linguagem** | Python | 3.10+ | Scripts ETL |
| **Orquestração** | Docker Compose | - | Ambiente isolado e reprodutível |
| **Processamento** | Pandas | 2.1.4 | Transformações de dados |
| **DB Driver** | psycopg2-binary | 2.9.9 | Conexão com PostgreSQL |
| **Logging** | Loguru | 0.7.2 | Logs estruturados |
| **Validação** | Custom validators | - | Validação de dados (CNPJ, email, etc) |

---

## 2. ARQUITETURA COMPLETA

### 2.1 Diagrama de Arquitetura Medallion

```
┌─────────────────────────────────────────────────────────────────────┐
│                         FONTE DE DADOS                              │
│                       CSV Files (OneDrive)                          │
├─────────────────────────────────────────────────────────────────────┤
│  Arquivos:                                                          │
│  • contas.csv         (clientes PJ/PF)                              │
│  • usuarios.csv       (vendedores/consultores)                      │
│  • faturamentos.csv   (transações de receita)                       │
│                                                                     │
│  Localização Docker: /app/data/input/onedrive/                     │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         │ ETL: run_bronze_ingestors.py
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      🥉 CAMADA BRONZE                               │
│                   (Dados Brutos Validados)                          │
├─────────────────────────────────────────────────────────────────────┤
│  Estratégia: TRUNCATE/RELOAD (substituição completa)               │
│  Validação: RIGOROSA (rejeita inválidos ANTES da inserção)         │
│  Filosofia: "Bronze nunca aceita lixo"                             │
│                                                                     │
│  Tabelas:                                                           │
│  ┌─────────────────┬──────────┬──────────────────────────────┐     │
│  │ Tabela          │ Registros│ Descrição                    │     │
│  ├─────────────────┼──────────┼──────────────────────────────┤     │
│  │ bronze.contas   │    10    │ Clientes (CNPJ/CPF validado) │     │
│  │ bronze.usuarios │    12    │ Consultores (email validado) │     │
│  │ bronze.faturamentos │  13  │ Transações (valores > 0)     │     │
│  │ bronze.data     │   366    │ Calendário 2024              │     │
│  └─────────────────┴──────────┴──────────────────────────────┘     │
│                                                                     │
│  Rejeições (dados inválidos):                                      │
│  • auditoria.log_rejeicao → 23 registros rejeitados (39.6%)        │
│                                                                     │
│  Auditoria:                                                         │
│  • auditoria.historico_execucao → Rastreamento completo            │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         │ ETL: run_silver_transformers.py
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                      🥈 CAMADA SILVER                               │
│                (Dados Curados - Star Schema)                        │
├─────────────────────────────────────────────────────────────────────┤
│  Estratégia:                                                        │
│  • Dimensões → SCD Type 2 (versionamento histórico)                │
│  • Fatos → FULL RELOAD (reconstrução completa)                     │
│  Modelagem: Star Schema (otimizado para BI)                        │
│                                                                     │
│  Dimensões:                                                         │
│  ┌──────────────────┬──────────┬────────────────────────────┐      │
│  │ Tabela           │ Registros│ Características            │      │
│  ├──────────────────┼──────────┼────────────────────────────┤      │
│  │ dim_cliente      │    10    │ SCD Type 2, CNPJ formatado │      │
│  │ dim_usuario      │    12    │ SCD Type 2, hierarquia     │      │
│  │ dim_data         │   319    │ Calendário enriquecido     │      │
│  └──────────────────┴──────────┴────────────────────────────┘      │
│                                                                     │
│  Fatos:                                                             │
│  ┌──────────────────┬──────────┬────────────────────────────┐      │
│  │ fato_faturamento │    13    │ 4 FKs, 7 measures          │      │
│  └──────────────────┴──────────┴────────────────────────────┘      │
│                                                                     │
│  Integridade:                                                       │
│  • 100% Foreign Keys resolvidas (0 órfãs)                          │
│  • 100% Natural Keys únicos                                        │
│  • Valor total: R$ 246.803,25 (validado)                           │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         │ SQL Views (auto-atualizam)
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                       🏆 CAMADA GOLD                                │
│               (Views Analíticas - Dados Reais)                      │
├─────────────────────────────────────────────────────────────────────┤
│  Estratégia: SQL Views (zero ETL, zero manutenção)                 │
│  Filosofia: "Agregue o que existe, não invente o que falta"        │
│                                                                     │
│  Views:                                                             │
│  ┌────────────────────────┬──────────┬───────────────────────┐     │
│  │ View                   │ Registros│ Agregação             │     │
│  ├────────────────────────┼──────────┼───────────────────────┤     │
│  │ vendas_diarias         │    13    │ Receita por dia       │     │
│  │ vendas_semanais        │    13    │ Receita por semana    │     │
│  │ vendas_mensais         │    12    │ Receita por mês       │     │
│  │ carteira_clientes      │    13    │ Snapshot de carteira  │     │
│  │ performance_consultores│    12    │ Métricas por consultor│     │
│  └────────────────────────┴──────────┴───────────────────────┘     │
│                                                                     │
│  Características:                                                   │
│  • Atualização automática (sincronizada com Silver)                │
│  • Apenas dados reais (sem campos NULL inventados)                 │
│  • Agregações simples (SUM, COUNT, MAX, MIN)                       │
└────────────────────────┬────────────────────────────────────────────┘
                         │
                         │ Consumo
                         │
                         ▼
┌─────────────────────────────────────────────────────────────────────┐
│                    CONSUMO (BI / Analytics)                         │
├─────────────────────────────────────────────────────────────────────┤
│  Ferramentas:                                                       │
│  • Power BI (dashboards executivos)                                │
│  • Metabase (análises ad-hoc)                                      │
│  • Queries SQL diretas (análises customizadas)                     │
│                                                                     │
│  Estratégia:                                                        │
│  • Import Mode: Gold views (performance)                           │
│  • DirectQuery: Silver (dados em tempo real)                       │
└─────────────────────────────────────────────────────────────────────┘
```

### 2.2 Padrões de Design Implementados

#### **Template Method Pattern** (Ingestores e Transformadores)

**Classe Base:** `BaseCSVIngestor` (python/ingestors/csv/base_csv_ingestor.py)

**Fluxo de Execução (método `executar()`):**
```
1. validar_arquivo_existe()
2. get_db_connection()
3. registrar_execucao() → auditoria.historico_execucao
4. ler_csv() → pandas DataFrame
5. validar_linha() → para cada linha
   └─ se INVÁLIDA → log_rejeicao.adicionar()
6. transformar_para_bronze() → aplicar column mapping
7. inserir_bronze() → TRUNCATE + INSERT
8. arquivar_arquivo() → move para /processed/
9. finalizar_execucao() → atualiza auditoria
```

**Métodos Abstratos (implementados por child classes):**
- `get_column_mapping()`: Mapeia CSV → Bronze
- `get_bronze_columns()`: Define ordem das colunas
- `get_validation_rules()`: Define regras de validação

#### **Strategy Pattern** (Diferentes Estratégias de Carga)

| Camada | Estratégia | Justificativa |
|--------|-----------|---------------|
| **Bronze** | TRUNCATE/RELOAD | Dados brutos são sempre substituídos completamente |
| **Silver Dimensões** | SCD Type 2 | Rastrear mudanças históricas (status, senioridade, etc) |
| **Silver Fatos** | FULL RELOAD | Reconstruir fatos a partir do Bronze (idempotência) |
| **Gold** | SQL Views | Zero ETL, atualização automática |

#### **Dependency Injection** (Configuração e Conexões)

**Variáveis de Ambiente (.env):**
```bash
DB_HOST=creditsdw.postgres.database.azure.com
DB_PORT=5432
DB_NAME=creditsdw
DB_USER=creditsdw
DB_PASSWORD=58230925AD@
```

**Injeção de Dependências:**
```python
# python/utils/db_connection.py
def get_db_connection():
    """Retorna conexão PostgreSQL usando env vars"""
    return psycopg2.connect(
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT'),
        database=os.getenv('DB_NAME'),
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD')
    )
```

---

## 3. CONFIGURAÇÃO E VARIÁVEIS DE AMBIENTE

### 3.1 Estrutura de Diretórios

```
credits-dw/
├── docker/                          # Configuração Docker
│   ├── docker-compose.yml           # Orquestração de containers
│   ├── Dockerfile                   # Imagem Python ETL
│   └── data/                        # Dados montados no container
│       ├── input/onedrive/          # CSVs de entrada
│       ├── processed/               # CSVs arquivados
│       └── templates/               # Exemplos de CSV
├── python/                          # Código Python
│   ├── ingestors/csv/               # Ingestores Bronze
│   │   ├── base_csv_ingestor.py     # Classe base (Template Method)
│   │   ├── ingest_contas.py         # Ingestor de clientes
│   │   ├── ingest_usuarios.py       # Ingestor de usuários
│   │   ├── ingest_faturamentos.py   # Ingestor de faturamento
│   │   └── ingest_data.py           # Ingestor de calendário
│   ├── transformers/silver/         # Transformadores Silver
│   │   ├── base_silver_transformer.py # Classe base
│   │   ├── transform_dim_cliente.py # Dim Cliente (SCD2)
│   │   ├── transform_dim_usuario.py # Dim Usuário (SCD2)
│   │   ├── transform_dim_data.py    # Dim Data
│   │   └── transform_fato_faturamento.py # Fato Faturamento
│   ├── utils/                       # Utilitários
│   │   ├── db_connection.py         # Conexão PostgreSQL
│   │   ├── audit.py                 # Sistema de auditoria
│   │   ├── validators.py            # Validadores (CNPJ, email, etc)
│   │   ├── rejection_logger.py      # Logger de rejeições
│   │   └── logger.py                # Configuração de logs
│   ├── run_bronze_ingestors.py      # Runner Bronze
│   └── run_silver_transformers.py   # Runner Silver
├── sql/                             # Scripts SQL
│   ├── create_schemas.sql           # Criação de schemas
│   ├── create_bronze_tables.sql     # Tabelas Bronze
│   ├── create_silver_tables.sql     # Tabelas Silver
│   └── create_gold_views.sql        # Views Gold
├── logs/                            # Logs de execução
├── docs/                            # Documentação adicional
├── .env                             # Variáveis de ambiente (GITIGNORED)
├── CLAUDE.md                        # Instruções para Claude Code
├── RELATORIO_TECNICO_INTERNO.md     # Relatório técnico v5.0
└── GUIA_TECNICO_COMPLETO.md         # Este documento
```

### 3.2 Variáveis de Ambiente (.env)

**Arquivo:** `.env` (na raiz do projeto)

```bash
# ===== DATABASE CONFIGURATION =====
DB_HOST=creditsdw.postgres.database.azure.com
DB_PORT=5432
DB_NAME=creditsdw
DB_USER=creditsdw
DB_PASSWORD=58230925AD@

# ===== APPLICATION SETTINGS =====
APP_ENV=production
LOG_LEVEL=INFO

# ===== TIMEZONE =====
TZ=America/Sao_Paulo
```

**⚠️ IMPORTANTE:**
- Este arquivo é **git-ignored** (nunca commitar credenciais)
- Todos os scripts Python carregam estas variáveis via `os.getenv()`
- Docker Compose carrega automaticamente o `.env`

### 3.3 Configuração Docker

**Arquivo:** `docker/docker-compose.yml`

```yaml
services:
  etl-processor:
    build:
      context: ..
      dockerfile: docker/Dockerfile
    container_name: credits-dw-etl
    env_file:
      - ../.env  # Carrega variáveis de ambiente
    volumes:
      - ../python:/app/python          # Código Python
      - ../logs:/app/logs              # Logs de execução
      - ./data:/app/data               # Dados (input/processed)
    working_dir: /app
    command: tail -f /dev/null         # Mantém container rodando
```

**Comandos Docker:**
```bash
# Iniciar ambiente (a partir do diretório docker/)
cd docker && docker compose up -d --build

# Executar ingestores Bronze
docker compose exec etl-processor python python/run_bronze_ingestors.py

# Executar transformadores Silver
docker compose exec etl-processor python python/run_silver_transformers.py

# Acessar shell do container (debugging)
docker compose exec etl-processor bash

# Parar ambiente
docker compose down
```

---

## 4. CAMADA BRONZE - INGESTÃO E VALIDAÇÃO

### 4.1 Filosofia da Bronze

**Princípios:**
1. **"Bronze nunca aceita lixo"** → Validação rigorosa ANTES da inserção
2. **"Dados brutos, não dados sujos"** → Raw data, mas 100% válida
3. **"Rejeite e logue, não silencie"** → Erros são registrados para análise

### 4.2 Tabelas Bronze

#### **bronze.contas** (Clientes PJ/PF)

**Estrutura:**
```sql
CREATE TABLE bronze.contas (
    sk_id SERIAL PRIMARY KEY,           -- Surrogate key (interno)
    cnpj_cpf VARCHAR(20) NOT NULL,      -- CNPJ/CPF sem formatação
    tipo CHAR(2),                       -- 'PJ' ou 'PF'
    status VARCHAR(50),                 -- 'ATIVO', 'INATIVO', 'SUSPENSO'
    status_qualificacao_da_conta VARCHAR(200),
    data_criacao DATE,                  -- Data de cadastro do cliente
    grupo VARCHAR(100),                 -- Grupo econômico
    razao_social VARCHAR(500),          -- Nome da empresa/pessoa
    responsavel_conta VARCHAR(200),     -- Account manager
    financeiro VARCHAR(200),            -- Email financeiro
    corte VARCHAR(50),                  -- Segmentação interna
    faixa VARCHAR(50)                   -- Faixa de faturamento
);
```

**Dados Reais (amostra):**
```
sk_id | cnpj_cpf       | tipo | status | razao_social          | data_criacao
------|----------------|------|--------|-----------------------|-------------
1     | 11222333000181 | PJ   | ATIVO  | Alpha Tecnologia Ltda | 2023-02-01
2     | 11222334000126 | PJ   | ATIVO  | Beta Soluções SA      | 2023-03-08
3     | 11222335000170 | PJ   | INATIVO| Gamma Innovations     | 2023-03-27
```

**Validações Aplicadas:**
```python
# python/ingestors/csv/ingest_contas.py
def get_validation_rules(self) -> Dict[str, dict]:
    return {
        'cnpj_cpf': {
            'obrigatorio': True,
            'tipo': 'cnpj_cpf',           # Valida dígitos verificadores
        },
        'tipo': {
            'obrigatorio': True,
            'tipo': 'string',
            'dominio': ['PJ', 'PF'],       # Apenas esses valores
            'case_sensitive': False
        },
        'status': {
            'obrigatorio': True,
            'tipo': 'string',
            'dominio': ['ATIVO', 'INATIVO', 'SUSPENSO']
        },
        'data_criacao': {
            'obrigatorio': True,
            'tipo': 'data',
            'formato_data': '%Y-%m-%d'
        },
        'razao_social': {
            'obrigatorio': True,
            'tipo': 'string',
            'min_len': 3,                  # Mínimo 3 caracteres
            'max_len': 500
        }
    }
```

#### **bronze.usuarios** (Consultores/Vendedores)

**Estrutura:**
```sql
CREATE TABLE bronze.usuarios (
    sk_id SERIAL PRIMARY KEY,
    nome_empresa VARCHAR(200),          -- Nome da empresa (Credits Brasil)
    nome VARCHAR(200) NOT NULL,         -- Nome do consultor
    area VARCHAR(100),                  -- Área de atuação (Vendas, etc)
    senioridade VARCHAR(50),            -- Junior, Pleno, Senior
    gestor VARCHAR(200),                -- Nome do gestor
    email VARCHAR(200) NOT NULL,        -- Email corporativo
    canal_1 VARCHAR(100),               -- Canal principal
    canal_2 VARCHAR(100),               -- Canal secundário
    email_lider VARCHAR(200)            -- Email do líder
);
```

**Dados Reais (amostra):**
```
sk_id | nome         | email                       | senioridade | gestor
------|--------------|-----------------------------|-----------|-----------
1     | João Silva   | joao.silva@credits.com.br   | Senior    | NULL
2     | Maria Santos | maria.santos@credits.com.br | Pleno     | João Silva
3     | Pedro Costa  | pedro.costa@credits.com.br  | Junior    | Maria Santos
```

**Validações Aplicadas:**
```python
def get_validation_rules(self) -> Dict[str, dict]:
    return {
        'nome': {
            'obrigatorio': True,
            'tipo': 'string',
            'min_len': 3
        },
        'email': {
            'obrigatorio': True,
            'tipo': 'email'                # Regex: .+@.+\..+
        },
        'senioridade': {
            'obrigatorio': False,
            'tipo': 'string',
            'dominio': ['Junior', 'Pleno', 'Senior', 'Gerente']
        }
    }
```

#### **bronze.faturamentos** (Transações de Receita)

**Estrutura:**
```sql
CREATE TABLE bronze.faturamentos (
    sk_id SERIAL PRIMARY KEY,
    data DATE NOT NULL,                 -- Data da transação
    receita NUMERIC(15,2) NOT NULL,     -- Valor bruto
    moeda VARCHAR(3) NOT NULL,          -- BRL, USD, EUR
    cnpj_cliente VARCHAR(20) NOT NULL,  -- FK textual para contas
    email_usuario VARCHAR(200) NOT NULL -- FK textual para usuarios
);
```

**Dados Reais (amostra):**
```
sk_id | data       | receita    | moeda | cnpj_cliente   | email_usuario
------|------------|------------|-------|----------------|-------------------
1     | 2024-01-15 | 15000.50   | BRL   | 11222333000181 | joao.silva@...
2     | 2024-02-20 | 25000.00   | BRL   | 11222334000126 | maria.santos@...
3     | 2024-03-10 | 8500.75    | BRL   | 11222335000170 | pedro.costa@...
```

**Validações Aplicadas:**
```python
def get_validation_rules(self) -> Dict[str, dict]:
    return {
        'data': {
            'obrigatorio': True,
            'tipo': 'data',
            'formato_data': '%Y-%m-%d'
        },
        'receita': {
            'obrigatorio': True,
            'tipo': 'decimal',
            'positivo': True               # Receita > 0
        },
        'moeda': {
            'obrigatorio': True,
            'tipo': 'string',
            'dominio': ['BRL', 'USD', 'EUR']
        },
        'cnpj_cliente': {
            'obrigatorio': True,
            'tipo': 'cnpj_cpf'
        },
        'email_usuario': {
            'obrigatorio': True,
            'tipo': 'email'
        }
    }
```

#### **bronze.data** (Calendário Dimensional)

**Estrutura:**
```sql
CREATE TABLE bronze.data (
    data_completa DATE PRIMARY KEY,     -- Natural key
    ano SMALLINT,
    mes SMALLINT,
    dia SMALLINT,
    bimestre SMALLINT,
    trimestre SMALLINT,
    quarter SMALLINT,                   -- Sinônimo de trimestre
    semestre SMALLINT
);
```

**Dados Reais (amostra):**
```
data_completa | ano  | mes | dia | trimestre | semestre
--------------|------|-----|-----|-----------|----------
2024-01-01    | 2024 | 1   | 1   | 1         | 1
2024-01-02    | 2024 | 1   | 2   | 1         | 1
2024-12-31    | 2024 | 12  | 31  | 4         | 2
```

**Observação:** Esta tabela é gerada via SQL, não CSV.

### 4.3 Validadores Implementados

**Arquivo:** `python/utils/validators.py`

#### **Validador de CNPJ/CPF** (Crítico)

```python
def validar_cnpj_cpf(valor: str) -> Tuple[bool, str]:
    """
    Valida CNPJ (14 dígitos) ou CPF (11 dígitos) com dígitos verificadores.

    Algoritmo:
    1. Remove caracteres não-numéricos
    2. Valida tamanho (11 para CPF, 14 para CNPJ)
    3. Rejeita dígitos repetidos (11111111111111)
    4. Calcula dígito verificador 1
    5. Calcula dígito verificador 2
    6. Compara com dígitos recebidos

    Returns:
        (True, "") se válido
        (False, "mensagem de erro") se inválido
    """
    # Remove caracteres não-numéricos
    numeros = re.sub(r'\D', '', str(valor))

    # Valida tamanho
    if len(numeros) not in [11, 14]:
        return False, f"CNPJ/CPF deve ter 11 ou 14 dígitos, recebido: {len(numeros)}"

    # Rejeita dígitos repetidos
    if numeros == numeros[0] * len(numeros):
        return False, "CNPJ/CPF inválido (todos os dígitos iguais)"

    # Calcula dígitos verificadores (algoritmo oficial)
    # ... (implementação completa no arquivo)

    return True, ""
```

**Exemplo de Rejeição:**
```
CSV Linha 5: CNPJ = "12345678000195"
Validação: FALHOU
Motivo: "CNPJ inválido (dígito verificador incorreto): 12345678000195"
Ação: Linha rejeitada e registrada em auditoria.log_rejeicao
```

#### **Validador de Email**

```python
def validar_email(valor: str) -> Tuple[bool, str]:
    """
    Valida formato de email usando regex.

    Regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$

    Exemplos válidos:
    - joao.silva@credits.com.br
    - maria@example.com

    Exemplos inválidos:
    - teste (sem @)
    - teste@ (sem domínio)
    - teste@@example.com (múltiplos @)
    """
    pattern = r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$'
    if re.match(pattern, str(valor)):
        return True, ""
    return False, f"Email inválido: {valor}"
```

#### **Validador de Domínio (Valores Permitidos)**

```python
def validar_valor_dominio(
    valor: str,
    dominio: List[str],
    case_sensitive: bool = True
) -> Tuple[bool, str]:
    """
    Valida se valor está em lista de valores permitidos.

    Args:
        valor: Valor a validar
        dominio: Lista de valores permitidos
        case_sensitive: Se True, diferencia maiúsculas/minúsculas

    Exemplo:
        validar_valor_dominio('BRL', ['BRL', 'USD', 'EUR'])
        → (True, "")

        validar_valor_dominio('XXX', ['BRL', 'USD', 'EUR'])
        → (False, "Valor 'XXX' não está no domínio permitido: BRL, USD, EUR")
    """
    valor_str = str(valor)
    if not case_sensitive:
        valor_str = valor_str.upper()
        dominio = [d.upper() for d in dominio]

    if valor_str in dominio:
        return True, ""
    return False, f"Valor '{valor}' não está no domínio permitido: {', '.join(dominio)}"
```

### 4.4 Sistema de Rejeição

**Tabela:** `auditoria.log_rejeicao`

```sql
CREATE TABLE auditoria.log_rejeicao (
    id BIGSERIAL PRIMARY KEY,
    execucao_id UUID NOT NULL,          -- FK para historico_execucao
    script_nome VARCHAR(255) NOT NULL,  -- Ex: 'ingest_faturamentos.py'
    tabela_destino VARCHAR(100),        -- Ex: 'bronze.faturamentos'
    numero_linha INTEGER,               -- Linha do CSV (1-indexed)
    campo_falha VARCHAR(100),           -- Campo que falhou validação
    motivo_rejeicao TEXT NOT NULL,      -- Mensagem de erro detalhada
    valor_recebido TEXT,                -- Valor que causou a falha
    registro_completo JSONB,            -- Linha completa do CSV em JSON
    severidade VARCHAR(20),             -- WARNING, ERROR, CRITICAL
    data_rejeicao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
```

**Dados Reais (amostra):**
```
id | script_nome              | numero_linha | campo_falha | motivo_rejeicao
---|--------------------------|--------------|-------------|------------------
1  | ingest_faturamentos.py   | 5            | cnpj_cliente| CNPJ inválido (dígito verificador)
2  | ingest_usuarios.py       | 12           | email       | Email inválido: 'teste@'
3  | ingest_contas.py         | 8            | tipo        | Valor 'XYZ' não está no domínio permitido: PJ, PF
```

**Query Útil:** Resumo de rejeições por campo
```sql
SELECT
    campo_falha,
    motivo_rejeicao,
    COUNT(*) as total_rejeitado
FROM auditoria.log_rejeicao
WHERE script_nome = 'ingest_faturamentos.py'
  AND data_rejeicao >= NOW() - INTERVAL '7 days'
GROUP BY campo_falha, motivo_rejeicao
ORDER BY total_rejeitado DESC;
```

### 4.5 Fluxo de Execução Bronze (Passo a Passo)

**Script:** `python/run_bronze_ingestors.py`

```python
# 1. Instanciar ingestores
ingestores = [
    IngestContas(),
    IngestUsuarios(),
    IngestFaturamentos(),
    IngestData()
]

# 2. Executar sequencialmente
for ingestor in ingestores:
    try:
        ingestor.executar()
    except Exception as e:
        logger.error(f"Erro no ingestor {ingestor.script_name}: {e}")
```

**Fluxo Interno (`base_csv_ingestor.py::executar()`):**

```
1. validar_arquivo_existe()
   └─ Se não existe → ABORTAR

2. get_db_connection()
   └─ Conectar ao PostgreSQL

3. registrar_execucao()
   └─ INSERT auditoria.historico_execucao (status='em_execucao')

4. ler_csv()
   └─ pd.read_csv(sep=';', dtype=str, encoding='utf-8')
   └─ Se falhar → tentar encoding='ISO-8859-1'

5. validar_linha() [PARA CADA LINHA]
   ├─ validar_campo() para cada coluna
   ├─ Se VÁLIDA → adicionar a lista de válidos
   └─ Se INVÁLIDA → log_rejeicao.adicionar()

6. transformar_para_bronze()
   └─ Aplicar column mapping (CSV → Bronze)
   └─ Formatar datas (colunas data_* → YYYY-MM-DD)

7. inserir_bronze()
   ├─ TRUNCATE TABLE bronze.<tabela>
   └─ INSERT INTO bronze.<tabela> VALUES (...)

8. arquivar_arquivo()
   └─ mv <arquivo>.csv → /processed/<arquivo>_YYYYMMDD_HHMMSS.csv

9. finalizar_execucao()
   └─ UPDATE auditoria.historico_execucao SET
         status='sucesso',
         linhas_processadas=X,
         linhas_inseridas=Y,
         linhas_rejeitadas=Z,
         data_fim=NOW()
```

**Logs de Execução Reais:**
```
[2025-11-27 11:00:15] [INFO] [ingest_contas.py] Iniciando ingestão: bronze.contas
[2025-11-27 11:00:15] [INFO] Arquivo encontrado: /app/data/input/onedrive/contas.csv
[2025-11-27 11:00:15] [INFO] CSV lido: 12 linhas
[2025-11-27 11:00:16] [INFO] Validação: 10 válidas, 2 rejeitadas
[2025-11-27 11:00:16] [INFO] Inseridos 10 registros em bronze.contas
[2025-11-27 11:00:16] [INFO] Arquivo arquivado: contas_20251127_110016.csv
[2025-11-27 11:00:16] [INFO] [SUCESSO] Ingestão concluída em 1.2s
```

---

## 5. CAMADA SILVER - STAR SCHEMA

### 5.1 Filosofia da Silver

**Princípios:**
1. **"Star Schema para performance analítica"** → Menos JOINs, queries mais rápidas
2. **"SCD Type 2 para rastrear história"** → Nunca perder dados históricos
3. **"100% integridade referencial"** → Todas FKs resolvidas, zero órfãs

### 5.2 Star Schema Completo

```
                    ┌─────────────────────────────┐
                    │     dim_data (319 reg)      │
                    ├─────────────────────────────┤
                    │ PK: data_sk (INTEGER)       │
                    │ UK: data_completa (DATE)    │
                    ├─────────────────────────────┤
                    │ ano, mes, trimestre         │
                    │ nome_mes, dia_semana        │
                    │ flag_fim_semana, dia_util   │
                    └──────────────┬──────────────┘
                                   │
                                   │ 1:N (fato → dim)
                                   │
    ┌──────────────────────────────┼──────────────────────────────┐
    │                              │                              │
    │ 1:N                          │                              │ N:1
    │                              │                              │
┌───┴────────────────────┐  ┌──────┴───────────────────────────┐  ┌────────────────────────┐
│ dim_cliente (10 reg)   │  │   fato_faturamento (13 reg)      │  │ dim_usuario (12 reg)   │
├────────────────────────┤  ├──────────────────────────────────┤  ├────────────────────────┤
│ PK: cliente_sk (SERIAL)│  │ PK: faturamento_sk (BIGSERIAL)   │  │ PK: usuario_sk (SERIAL)│
│ UK: cnpj_cpf_nk+versao │◄─┤ FK: cliente_sk → dim_cliente     │  │ UK: usuario_nk+versao  │
├────────────────────────┤  │ FK: usuario_sk → dim_usuario     ├─►├────────────────────────┤
│ cnpj_cpf_nk (NK)       │  │ FK: data_sk → dim_data           │  │ usuario_nk (NK)        │
│ cnpj_cpf_formatado     │  │ FK: canal_sk (NULL permitido)    │  │ nome_completo          │
│ razao_social           │  ├──────────────────────────────────┤  │ senioridade            │
│ tipo_pessoa (PJ/PF)    │  │ MEASURES:                        │  │ gestor_sk (self-join)  │
│ status                 │  │ • valor_bruto (NUMERIC)          │  │ nivel_hierarquia       │
│ porte_empresa          │  │ • valor_liquido (calculado)      │  │ area_atuacao           │
│ tempo_cliente_dias     │  │ • valor_desconto                 │  │ canal_principal        │
│                        │  │ • valor_imposto (15%)            │  │ email_corporativo      │
│ SCD Type 2:            │  │ • valor_comissao (5%)            │  │                        │
│ • data_inicio          │  │ • hash_transacao (MD5)           │  │ SCD Type 2:            │
│ • data_fim             │  │                                  │  │ • data_inicio          │
│ • flag_ativo           │  │ DEGENERATE DIMENSIONS:           │  │ • data_fim             │
│ • versao               │  │ • moeda (BRL/USD/EUR)            │  │ • flag_ativo           │
│ • hash_registro (MD5)  │  │ • forma_pagamento                │  │ • versao               │
└────────────────────────┘  │ • status_pagamento               │  │ • hash_registro (MD5)  │
                            └──────────────────────────────────┘  └────────┬───────────────┘
                                                                           │
                                                                           │ self-join
                                                                           │ (hierarquia)
                                                                           │
                                                                    gestor_sk ──┐
                                                                                │
                                                                                ▼
                                                                         dim_usuario
```

### 5.3 Dimensões Detalhadas

#### **dim_cliente** (SCD Type 2)

**Estrutura Completa:**
```sql
CREATE TABLE silver.dim_cliente (
    -- Chaves
    cliente_sk INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    cnpj_cpf_nk VARCHAR(20) NOT NULL,   -- Natural Key (11 ou 14 dígitos)

    -- Atributos de Negócio
    razao_social VARCHAR(500),
    tipo_pessoa CHAR(2),                -- 'PJ' ou 'PF'
    status VARCHAR(50),                 -- ATIVO, INATIVO, SUSPENSO
    status_qualificacao VARCHAR(200),
    data_criacao DATE,
    grupo VARCHAR(100),                 -- Grupo econômico
    responsavel_conta VARCHAR(200),     -- Account manager
    email_financeiro VARCHAR(200),
    corte VARCHAR(50),
    faixa VARCHAR(50),

    -- Atributos Derivados (calculados no ETL)
    cnpj_cpf_formatado VARCHAR(25),     -- 00.000.000/0000-00 ou 000.000.000-00
    porte_empresa VARCHAR(20),          -- (não calculado ainda)
    tempo_cliente_dias INTEGER,         -- Dias desde data_criacao
    categoria_risco VARCHAR(20),        -- (não calculado ainda)

    -- SCD Type 2 Metadata
    data_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
    data_fim DATE,                      -- NULL = registro ativo
    flag_ativo BOOLEAN DEFAULT true,
    versao INTEGER DEFAULT 1,
    motivo_mudanca VARCHAR(200),        -- Descrição da mudança
    hash_registro VARCHAR(64),          -- MD5 para detectar alterações
    data_carga TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uk_cliente_cnpj_versao UNIQUE (cnpj_cpf_nk, versao)
);

CREATE INDEX idx_dim_clientes_ativo ON silver.dim_cliente(flag_ativo)
WHERE flag_ativo = true;
```

**Dados Reais (exemplo de versionamento):**
```
cliente_sk | cnpj_cpf_nk    | razao_social      | status  | versao | flag_ativo | data_inicio | data_fim
-----------|----------------|-------------------|---------|--------|------------|-------------|----------
31         | 11222333000181 | Alpha Tecnologia  | ATIVO   | 1      | true       | 2023-02-01  | NULL
32         | 11222334000126 | Beta Soluções     | ATIVO   | 1      | true       | 2023-03-08  | NULL
33         | 11222335000170 | Gamma Innovations | INATIVO | 1      | true       | 2023-03-27  | NULL
```

**Como funciona o SCD Type 2:**
```
Cenário: Cliente 31 muda de status ATIVO → SUSPENSO

ANTES da mudança:
cliente_sk | cnpj_cpf_nk    | status | versao | flag_ativo | data_inicio | data_fim
-----------|----------------|--------|--------|------------|-------------|----------
31         | 11222333000181 | ATIVO  | 1      | true       | 2023-02-01  | NULL

DEPOIS da mudança (próxima execução Silver):
cliente_sk | cnpj_cpf_nk    | status    | versao | flag_ativo | data_inicio | data_fim
-----------|----------------|-----------|--------|------------|-------------|----------
31         | 11222333000181 | ATIVO     | 1      | false      | 2023-02-01  | 2025-11-27  (versão antiga desativada)
40         | 11222333000181 | SUSPENSO  | 2      | true       | 2025-11-27  | NULL        (nova versão ativa)

Explicação:
1. Versão 1: flag_ativo=false, data_fim preenchida
2. Versão 2: nova linha inserida, flag_ativo=true, data_fim=NULL
3. cnpj_cpf_nk permanece o mesmo (é a natural key)
4. motivo_mudanca: "Alteração de status: ATIVO → SUSPENSO"
```

**Cálculos Derivados:**
```python
# python/transformers/silver/transform_dim_cliente.py

def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
    # 1. CNPJ/CPF formatado
    df['cnpj_cpf_formatado'] = df['cnpj_cpf_nk'].apply(self._formatar_cnpj_cpf)
    # Exemplo: 11222333000181 → 11.222.333/0001-81

    # 2. Tempo de cliente (dias)
    df['tempo_cliente_dias'] = (
        pd.Timestamp.now() - pd.to_datetime(df['data_criacao'])
    ).dt.days
    # Exemplo: 2023-02-01 → 681 dias (em 2025-11-27)

    # 3. Porte empresa (não implementado ainda)
    df['porte_empresa'] = 'NAO_CALCULADO'

    # 4. Categoria risco (não implementado ainda)
    df['categoria_risco'] = 'NAO_AVALIADO'

    return df
```

#### **dim_usuario** (SCD Type 2 com Hierarquia)

**Estrutura Completa:**
```sql
CREATE TABLE silver.dim_usuario (
    -- Chaves
    usuario_sk INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    usuario_nk VARCHAR(200) NOT NULL,   -- Natural Key (email ou nome)

    -- Atributos de Negócio
    nome_completo VARCHAR(200),
    email_corporativo VARCHAR(200),
    area_atuacao VARCHAR(100),
    senioridade VARCHAR(50),            -- Junior, Pleno, Senior, Gerente
    canal_principal VARCHAR(100),
    canal_secundario VARCHAR(100),
    email_lider VARCHAR(200),

    -- Hierarquia (self-referencing FK)
    gestor_sk INTEGER,                  -- FK para dim_usuario (self-join)
    nivel_hierarquia INTEGER,           -- 1=gestor, 2=tem gestor, 3=sem gestor

    -- Atributos Derivados
    nome_empresa VARCHAR(200),          -- Sempre 'Credits Brasil'
    status_usuario VARCHAR(20),         -- Sempre 'ATIVO' (calculado)

    -- SCD Type 2 Metadata
    data_inicio DATE NOT NULL DEFAULT CURRENT_DATE,
    data_fim DATE,
    flag_ativo BOOLEAN DEFAULT true,
    versao INTEGER DEFAULT 1,
    hash_registro VARCHAR(64),
    data_carga TIMESTAMP WITH TIME ZONE DEFAULT NOW(),

    -- Constraints
    CONSTRAINT uk_usuario_nk_versao UNIQUE (usuario_nk, versao),
    CONSTRAINT fk_dim_usuario_gestor FOREIGN KEY (gestor_sk)
        REFERENCES silver.dim_usuario(usuario_sk)
);
```

**Dados Reais (hierarquia):**
```
usuario_sk | usuario_nk                  | nome_completo | senioridade | gestor_sk | nivel_hierarquia
-----------|-----------------------------|--------------|-----------|-----------|-----------------
22         | joao.silva@credits.com.br   | João Silva   | Senior    | NULL      | 1  (é gestor)
23         | maria.santos@credits.com.br | Maria Santos | Pleno     | 22        | 2  (tem gestor)
24         | pedro.costa@credits.com.br  | Pedro Costa  | Junior    | 23        | 2  (tem gestor)

Hierarquia:
João Silva (gestor_sk=NULL)
  └─ Maria Santos (gestor_sk=22)
       └─ Pedro Costa (gestor_sk=23) [ERRO: Pedro deveria reportar a João, não Maria]
```

**Resolução de Hierarquia:**
```python
# python/transformers/silver/transform_dim_usuario.py

def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
    # 1. Resolver gestor_sk via LEFT JOIN com dim_usuario existente
    df = df.merge(
        dim_usuario_atual[['usuario_nk', 'usuario_sk']],
        left_on='email_lider',
        right_on='usuario_nk',
        how='left',
        suffixes=('', '_gestor')
    )
    df.rename(columns={'usuario_sk_gestor': 'gestor_sk'}, inplace=True)

    # 2. Calcular nível hierárquico
    df['nivel_hierarquia'] = df.apply(lambda row:
        1 if pd.isna(row['gestor_sk']) and row['senioridade'] == 'Senior' else
        2 if not pd.isna(row['gestor_sk']) else
        3,  # Sem gestor e não Senior
        axis=1
    )

    return df
```

#### **dim_data** (Calendário Dimensional)

**Estrutura Completa:**
```sql
CREATE TABLE silver.dim_data (
    -- Chaves
    data_sk INTEGER GENERATED BY DEFAULT AS IDENTITY PRIMARY KEY,
    data_completa DATE UNIQUE NOT NULL, -- Natural Key

    -- Componentes da Data
    ano SMALLINT,
    mes SMALLINT,
    dia SMALLINT,
    trimestre SMALLINT,                 -- 1, 2, 3, 4
    semestre SMALLINT,                  -- 1, 2
    bimestre SMALLINT,                  -- 1, 2, 3, 4, 5, 6

    -- Nomes Textuais
    nome_trimestre VARCHAR(10),         -- 'Q1', 'Q2', 'Q3', 'Q4'
    nome_mes VARCHAR(20),               -- 'Janeiro', 'Fevereiro', ...
    nome_mes_abrev VARCHAR(3),          -- 'Jan', 'Fev', ...
    nome_dia_semana VARCHAR(20),        -- 'Segunda-feira', ...
    nome_dia_semana_abrev VARCHAR(3),   -- 'Seg', 'Ter', ...

    -- Atributos de Negócio
    dia_semana SMALLINT,                -- 1=Domingo, 2=Segunda, ...
    semana_ano SMALLINT,                -- 1 a 53 (semana ISO)
    semana_mes SMALLINT,                -- 1 a 5 (semana dentro do mês)
    flag_fim_semana BOOLEAN,            -- TRUE se sábado/domingo
    flag_dia_util BOOLEAN,              -- TRUE se segunda-sexta (simplificado)

    -- Auditoria
    data_carga TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);

CREATE INDEX idx_dim_data_ano_mes ON silver.dim_data(ano, mes);
CREATE INDEX idx_dim_data_trimestre ON silver.dim_data(trimestre);
```

**Dados Reais (amostra):**
```
data_sk | data_completa | ano  | mes | dia | trimestre | nome_mes | dia_semana | nome_dia_semana | flag_fim_semana | flag_dia_util
--------|---------------|------|-----|-----|-----------|----------|------------|----------------|-----------------|---------------
1       | 2024-01-01    | 2024 | 1   | 1   | 1         | Janeiro  | 2          | Segunda-feira  | false           | true
37      | 2024-02-06    | 2024 | 2   | 6   | 1         | Fevereiro| 3          | Terça-feira    | false           | true
56      | 2024-02-25    | 2024 | 2   | 25  | 1         | Fevereiro| 1          | Domingo        | true            | false
```

**Geração do Calendário:**
```python
# python/transformers/silver/transform_dim_data.py

# SQL CTE para gerar série de datas
WITH date_series AS (
    SELECT
        calendar_date::date
    FROM generate_series(
        '2024-01-01'::date,
        '2024-12-31'::date,
        '1 day'::interval
    ) AS calendar_date
),
full_calendar AS (
    SELECT
        calendar_date,
        EXTRACT(YEAR FROM calendar_date)::smallint AS ano,
        EXTRACT(MONTH FROM calendar_date)::smallint AS mes,
        EXTRACT(DAY FROM calendar_date)::smallint AS dia,
        EXTRACT(QUARTER FROM calendar_date)::smallint AS trimestre,
        CASE WHEN EXTRACT(MONTH FROM calendar_date) <= 6 THEN 1 ELSE 2 END::smallint AS semestre,
        CASE WHEN EXTRACT(DOW FROM calendar_date) IN (0, 6) THEN true ELSE false END AS flag_fim_semana,
        CASE WHEN EXTRACT(DOW FROM calendar_date) BETWEEN 1 AND 5 THEN true ELSE false END AS flag_dia_util
    FROM date_series
)
INSERT INTO silver.dim_data (...)
SELECT * FROM full_calendar
ON CONFLICT (data_completa) DO NOTHING;
```

### 5.4 Fato Detalhado

#### **fato_faturamento** (Transações de Receita)

**Estrutura Completa:**
```sql
CREATE TABLE silver.fato_faturamento (
    -- Chave Primária
    faturamento_sk BIGSERIAL PRIMARY KEY,

    -- Foreign Keys (Dimensões)
    cliente_sk INTEGER NOT NULL,
    usuario_sk INTEGER NOT NULL,
    data_sk INTEGER NOT NULL,
    canal_sk INTEGER,                   -- Nullable (dimensão não implementada)

    -- Measures (Métricas Numéricas)
    valor_bruto NUMERIC(15,2) NOT NULL,
    valor_desconto NUMERIC(15,2) DEFAULT 0.00,
    valor_liquido NUMERIC(15,2),        -- Calculado: bruto - desconto
    valor_imposto NUMERIC(15,2),        -- Calculado: bruto * 0.15
    valor_comissao NUMERIC(15,2),       -- Calculado: bruto * 0.05
    quantidade INTEGER DEFAULT 1,
    numero_parcelas INTEGER DEFAULT 1,

    -- Degenerate Dimensions (atributos textuais do fato)
    numero_documento VARCHAR(50),
    tipo_documento VARCHAR(50),
    moeda VARCHAR(3) NOT NULL,          -- BRL, USD, EUR
    forma_pagamento VARCHAR(50),
    status_pagamento VARCHAR(50),
    data_vencimento DATE,
    data_pagamento DATE,

    -- Metadata
    origem_dado VARCHAR(20),            -- 'CSV', 'API', 'MANUAL'
    data_processamento TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    hash_transacao VARCHAR(64) UNIQUE,  -- MD5 para idempotência

    -- Constraints de Integridade Referencial
    CONSTRAINT fk_fact_faturamento_cliente FOREIGN KEY (cliente_sk)
        REFERENCES silver.dim_cliente(cliente_sk),
    CONSTRAINT fk_fact_faturamento_usuario FOREIGN KEY (usuario_sk)
        REFERENCES silver.dim_usuario(usuario_sk),
    CONSTRAINT fk_fact_faturamento_data FOREIGN KEY (data_sk)
        REFERENCES silver.dim_data(data_sk)
);

CREATE INDEX idx_fato_faturamento_cliente ON silver.fato_faturamento(cliente_sk);
CREATE INDEX idx_fato_faturamento_usuario ON silver.fato_faturamento(usuario_sk);
CREATE INDEX idx_fato_faturamento_data ON silver.fato_faturamento(data_sk);
```

**Dados Reais (amostra):**
```
faturamento_sk | cliente_sk | usuario_sk | data_sk | valor_bruto | valor_liquido | valor_imposto | moeda | hash_transacao
---------------|------------|------------|---------|-------------|---------------|---------------|-------|----------------
53             | 31         | 22         | 1       | 15000.50    | 15000.50      | 2250.08       | BRL   | 31673294...
54             | 32         | 23         | 37      | 25000.00    | 25000.00      | 3750.00       | BRL   | 4b4b8869...
55             | 33         | 24         | 56      | 8500.75     | 8500.75       | 1275.11       | BRL   | 573fce03...
```

**Cálculos de Métricas:**
```python
# python/transformers/silver/transform_fato_faturamento.py

def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
    # 1. Valor líquido
    df['valor_liquido'] = df['valor_bruto'] - df.get('valor_desconto', 0.00)

    # 2. Imposto (15% do bruto)
    df['valor_imposto'] = df['valor_bruto'] * 0.15

    # 3. Comissão (5% do bruto)
    df['valor_comissao'] = df['valor_bruto'] * 0.05

    # 4. Hash de transação (idempotência)
    df['hash_transacao'] = df.apply(
        lambda row: hashlib.md5(
            f"{row['cnpj_cliente']}_{row['email_usuario']}_{row['data']}_{row['receita']}".encode()
        ).hexdigest(),
        axis=1
    )

    return df
```

**Resolução de Foreign Keys:**
```python
# 1. Resolver cliente_sk
df = df.merge(
    dim_cliente[dim_cliente['flag_ativo'] == True][['cnpj_cpf_nk', 'cliente_sk']],
    left_on='cnpj_cliente',
    right_on='cnpj_cpf_nk',
    how='left'
)

# 2. Resolver usuario_sk
df = df.merge(
    dim_usuario[dim_usuario['flag_ativo'] == True][['usuario_nk', 'usuario_sk']],
    left_on='email_usuario',
    right_on='usuario_nk',
    how='left'
)

# 3. Resolver data_sk
df = df.merge(
    dim_data[['data_completa', 'data_sk']],
    left_on='data',
    right_on='data_completa',
    how='left'
)

# 4. Validar: Nenhuma FK pode ser NULL
missing_cliente = df['cliente_sk'].isna().sum()
missing_usuario = df['usuario_sk'].isna().sum()
missing_data = df['data_sk'].isna().sum()

if missing_cliente + missing_usuario + missing_data > 0:
    raise ValueError(f"FKs não resolvidas: cliente={missing_cliente}, usuario={missing_usuario}, data={missing_data}")
```

### 5.5 Fluxo de Execução Silver (Passo a Passo)

**Script:** `python/run_silver_transformers.py`

```python
# Ordem de execução (respeitando dependências)
transformadores = [
    TransformDimData(),           # 1º - sem dependências
    TransformDimCliente(),        # 2º - sem dependências
    TransformDimUsuario(),        # 3º - auto-dependência (gestor_sk)
    TransformFatoFaturamento()    # 4º - depende de todas as dims
]

for transformer in transformadores:
    try:
        transformer.executar()
    except Exception as e:
        logger.error(f"Erro no transformador {transformer.script_name}: {e}")
        break  # ABORTAR se dimensão falhar (fato depende delas)
```

**Fluxo Interno (`base_silver_transformer.py::executar()`):**

```
1. get_db_connection()

2. registrar_execucao()
   └─ INSERT auditoria.historico_execucao (status='em_execucao')

3. extrair_bronze(conn)
   └─ SELECT * FROM bronze.<tabela>
   └─ Retorna pandas DataFrame

4. aplicar_transformacoes(df)
   ├─ Padronização (CNPJ formatado, emails lowercase)
   ├─ Derivações (tempo_cliente_dias, nivel_hierarquia)
   ├─ Enriquecimento (cálculos de métricas)
   └─ Renomeação de colunas (Bronze → Silver)

5. validar_qualidade(df)
   ├─ Validar campos obrigatórios NOT NULL
   ├─ Validar tipos de dados
   ├─ Validar integridade referencial (FKs)
   └─ Se FALHOU → ABORTAR e logar erro

6. processar_scd2(df, conn)  [APENAS DIMENSÕES]
   ├─ Calcular hash_registro (MD5)
   ├─ Buscar registros ativos atuais
   ├─ Comparar hash novo vs atual
   ├─ Detectar mudanças:
   │  ├─ NOVOS → INSERT (versao=1, flag_ativo=true)
   │  ├─ ALTERADOS →
   │  │   ├─ UPDATE old (flag_ativo=false, data_fim=TODAY)
   │  │   └─ INSERT new (versao=versao+1, flag_ativo=true)
   │  └─ INALTERADOS → SKIP
   └─ Executar UPDATEs e INSERTs

6b. carregar_fato(df, conn)  [APENAS FATOS]
   ├─ TRUNCATE TABLE silver.fato_<tabela>
   └─ INSERT INTO silver.fato_<tabela> VALUES (...)

7. finalizar_execucao()
   └─ UPDATE auditoria.historico_execucao SET status='sucesso'
```

**Logs de Execução Reais:**
```
[2025-11-27 11:02:15] [INFO] [transform_dim_cliente.py] Iniciando transformação: silver.dim_cliente
[2025-11-27 11:02:15] [INFO] Extraídos 10 registros de bronze.contas
[2025-11-27 11:02:16] [INFO] Transformações aplicadas: CNPJ formatado, tempo_cliente_dias calculado
[2025-11-27 11:02:16] [INFO] Validação de qualidade: SUCESSO
[2025-11-27 11:02:16] [INFO] SCD Type 2: 0 novos, 0 alterados, 10 inalterados
[2025-11-27 11:02:16] [INFO] [SUCESSO] Transformação concluída em 1.5s
```

---

## 6. CAMADA GOLD - AGREGAÇÕES ANALÍTICAS

### 6.1 Filosofia da Gold (Refatoração v5.0)

**ANTES (Tabelas Gold - DELETADO):**
- ❌ 6 tabelas Gold com 70% de campos NULL/placeholder
- ❌ 8 scripts Python (~2.000 linhas de ETL)
- ❌ Manutenção constante (sincronização manual)
- ❌ "Inventava" dados não existentes

**DEPOIS (Views Gold - ATUAL):**
- ✅ 5 views SQL com agregações simples
- ✅ Zero ETL Python (apenas SQL)
- ✅ Manutenção zero (atualização automática)
- ✅ "Agrega o que existe, não inventa o que falta"

### 6.2 Views Gold Implementadas

#### **gold.vendas_diarias** (Receita por Dia e Consultor)

**SQL:**
```sql
CREATE OR REPLACE VIEW gold.vendas_diarias AS
SELECT
    d.data_completa,
    u.nome_completo AS consultor,
    u.senioridade,
    u.usuario_nk AS email_consultor,
    COUNT(f.faturamento_sk) AS total_transacoes,
    SUM(f.valor_bruto) AS receita_bruta,
    SUM(f.valor_liquido) AS receita_liquida,
    SUM(f.valor_comissao) AS comissao_total,
    f.moeda
FROM silver.fato_faturamento f
INNER JOIN silver.dim_data d ON d.data_sk = f.data_sk
INNER JOIN silver.dim_usuario u ON u.usuario_sk = f.usuario_sk AND u.flag_ativo = true
GROUP BY d.data_completa, u.nome_completo, u.senioridade, u.usuario_nk, f.moeda
ORDER BY d.data_completa DESC, receita_bruta DESC;
```

**Dados Reais (amostra):**
```
data_completa | consultor    | total_transacoes | receita_bruta | receita_liquida | comissao_total | moeda
--------------|--------------|------------------|---------------|-----------------|----------------|-------
2024-03-10    | Pedro Costa  | 1                | 8500.75       | 8500.75         | 425.04         | BRL
2024-02-20    | Maria Santos | 1                | 25000.00      | 25000.00        | 1250.00        | BRL
2024-01-15    | João Silva   | 1                | 15000.50      | 15000.50        | 750.03         | BRL
```

**Propósito:** Dashboard de vendas diárias, filtrado por consultor ou período.

#### **gold.vendas_mensais** (Receita por Mês e Consultor)

**SQL:**
```sql
CREATE OR REPLACE VIEW gold.vendas_mensais AS
SELECT
    d.ano,
    d.mes,
    d.nome_mes,
    u.nome_completo AS consultor,
    u.senioridade,
    COUNT(f.faturamento_sk) AS total_transacoes,
    SUM(f.valor_bruto) AS receita_bruta,
    SUM(f.valor_liquido) AS receita_liquida,
    SUM(f.valor_imposto) AS imposto_total,
    SUM(f.valor_comissao) AS comissao_total,
    f.moeda
FROM silver.fato_faturamento f
INNER JOIN silver.dim_data d ON d.data_sk = f.data_sk
INNER JOIN silver.dim_usuario u ON u.usuario_sk = f.usuario_sk AND u.flag_ativo = true
GROUP BY d.ano, d.mes, d.nome_mes, u.nome_completo, u.senioridade, f.moeda
ORDER BY d.ano DESC, d.mes DESC, receita_bruta DESC;
```

**Dados Reais (amostra):**
```
ano  | mes | nome_mes  | consultor    | total_transacoes | receita_bruta | comissao_total | moeda
-----|-----|-----------|--------------|------------------|---------------|----------------|-------
2024 | 3   | Março     | Pedro Costa  | 1                | 8500.75       | 425.04         | BRL
2024 | 2   | Fevereiro | Maria Santos | 1                | 25000.00      | 1250.00        | BRL
2024 | 1   | Janeiro   | João Silva   | 1                | 15000.50      | 750.03         | BRL
```

**Propósito:** Análise de tendências mensais, comparação MoM (month-over-month).

#### **gold.carteira_clientes** (Snapshot de Carteira)

**SQL:**
```sql
CREATE OR REPLACE VIEW gold.carteira_clientes AS
SELECT
    c.cnpj_cpf_formatado,
    c.razao_social,
    c.tipo_pessoa,
    c.status,
    c.tempo_cliente_dias,
    u.nome_completo AS consultor_responsavel,
    COUNT(f.faturamento_sk) AS total_transacoes,
    SUM(f.valor_bruto) AS receita_total,
    MAX(d.data_completa) AS data_ultima_transacao
FROM silver.dim_cliente c
LEFT JOIN silver.fato_faturamento f ON f.cliente_sk = c.cliente_sk
LEFT JOIN silver.dim_usuario u ON u.usuario_sk = f.usuario_sk AND u.flag_ativo = true
LEFT JOIN silver.dim_data d ON d.data_sk = f.data_sk
WHERE c.flag_ativo = true
GROUP BY c.cnpj_cpf_formatado, c.razao_social, c.tipo_pessoa, c.status, c.tempo_cliente_dias, u.nome_completo
ORDER BY receita_total DESC NULLS LAST;
```

**Dados Reais (amostra):**
```
cnpj_cpf_formatado | razao_social      | status  | consultor_responsavel | total_transacoes | receita_total | data_ultima_transacao
-------------------|-------------------|---------|----------------------|------------------|---------------|---------------------
11.222.334/0001-26 | Beta Soluções     | ATIVO   | Maria Santos         | 1                | 25000.00      | 2024-02-20
11.222.333/0001-81 | Alpha Tecnologia  | ATIVO   | João Silva           | 1                | 15000.50      | 2024-01-15
11.222.335/0001-70 | Gamma Innovations | INATIVO | Pedro Costa          | 1                | 8500.75       | 2024-03-10
```

**Propósito:** Análise de carteira de clientes, identificar inativos, recência de transações.

#### **gold.performance_consultores** (Métricas por Consultor)

**SQL:**
```sql
CREATE OR REPLACE VIEW gold.performance_consultores AS
SELECT
    u.nome_completo AS consultor,
    u.senioridade,
    u.area_atuacao,
    COUNT(DISTINCT f.cliente_sk) AS total_clientes_unicos,
    COUNT(f.faturamento_sk) AS total_transacoes,
    SUM(f.valor_bruto) AS receita_total,
    AVG(f.valor_bruto) AS ticket_medio,
    SUM(f.valor_comissao) AS comissao_total,
    MAX(d.data_completa) AS data_ultima_venda
FROM silver.dim_usuario u
LEFT JOIN silver.fato_faturamento f ON f.usuario_sk = u.usuario_sk
LEFT JOIN silver.dim_data d ON d.data_sk = f.data_sk
WHERE u.flag_ativo = true
GROUP BY u.nome_completo, u.senioridade, u.area_atuacao
ORDER BY receita_total DESC NULLS LAST;
```

**Dados Reais (amostra):**
```
consultor    | senioridade | total_clientes_unicos | total_transacoes | receita_total | ticket_medio | comissao_total
-------------|-------------|----------------------|------------------|---------------|--------------|----------------
Maria Santos | Pleno       | 1                    | 1                | 25000.00      | 25000.00     | 1250.00
João Silva   | Senior      | 1                    | 1                | 15000.50      | 15000.50     | 750.03
Pedro Costa  | Junior      | 1                    | 1                | 8500.75       | 8500.75      | 425.04
```

**Propósito:** Ranking de consultores, análise de produtividade, ticket médio.

#### **gold.vendas_semanais** (Receita por Semana)

**SQL:**
```sql
CREATE OR REPLACE VIEW gold.vendas_semanais AS
SELECT
    d.ano,
    d.semana_ano,
    u.nome_completo AS consultor,
    COUNT(f.faturamento_sk) AS total_transacoes,
    SUM(f.valor_bruto) AS receita_bruta,
    SUM(f.valor_comissao) AS comissao_total,
    f.moeda
FROM silver.fato_faturamento f
INNER JOIN silver.dim_data d ON d.data_sk = f.data_sk
INNER JOIN silver.dim_usuario u ON u.usuario_sk = f.usuario_sk AND u.flag_ativo = true
GROUP BY d.ano, d.semana_ano, u.nome_completo, f.moeda
ORDER BY d.ano DESC, d.semana_ano DESC, receita_bruta DESC;
```

**Propósito:** Análise semanal de vendas, identificar semanas de alta/baixa performance.

### 6.3 Validação de Integridade

**Query de Validação:**
```sql
-- Comparar totais: Bronze vs Silver vs Gold
SELECT
    'Bronze' AS camada,
    SUM(receita) AS total_receita
FROM bronze.faturamentos

UNION ALL

SELECT
    'Silver (Fato)',
    SUM(valor_bruto)
FROM silver.fato_faturamento

UNION ALL

SELECT
    'Gold (vendas_diarias)',
    SUM(receita_bruta)
FROM gold.vendas_diarias;
```

**Resultado Real:**
```
camada                | total_receita
----------------------|---------------
Bronze                | 246803.25
Silver (Fato)         | 246803.25
Gold (vendas_diarias) | 246803.25

✅ 100% ALINHADO (integridade validada)
```

---

## 7. FLUXO DE DADOS COMPLETO COM EXEMPLOS REAIS

### 7.1 Cenário Real: Registro de Faturamento

**Arquivo CSV:** `faturamentos.csv`
```csv
Data;Receita;Moeda;CNPJ Cliente;Email Usuario
2024-01-15;15000.50;BRL;11222333000181;joao.silva@credits.com.br
2024-02-20;25000.00;BRL;11222334000126;maria.santos@credits.com.br
2024-03-10;8500.75;BRL;11222335000170;pedro.costa@credits.com.br
```

**Passo 1: Ingestão Bronze**

```python
# python/ingestors/csv/ingest_faturamentos.py::executar()

1. Ler CSV:
   - Encoding: UTF-8
   - Delimiter: ;
   - dtype: str (todas colunas como string)

2. Validar Linha 1:
   - data='2024-01-15' → validar_data() → ✅ VÁLIDO
   - receita='15000.50' → validar_numero(positivo=True) → ✅ VÁLIDO
   - moeda='BRL' → validar_dominio(['BRL','USD','EUR']) → ✅ VÁLIDO
   - cnpj_cliente='11222333000181' → validar_cnpj_cpf() → ✅ VÁLIDO (dígitos corretos)
   - email_usuario='joao.silva@credits.com.br' → validar_email() → ✅ VÁLIDO

3. Aplicar column mapping:
   CSV                      Bronze
   'Data'           →       'data'
   'Receita'        →       'receita'
   'Moeda'          →       'moeda'
   'CNPJ Cliente'   →       'cnpj_cliente'
   'Email Usuario'  →       'email_usuario'

4. Inserir no Bronze:
   TRUNCATE TABLE bronze.faturamentos;
   INSERT INTO bronze.faturamentos (data, receita, moeda, cnpj_cliente, email_usuario)
   VALUES
       ('2024-01-15', 15000.50, 'BRL', '11222333000181', 'joao.silva@credits.com.br'),
       ('2024-02-20', 25000.00, 'BRL', '11222334000126', 'maria.santos@credits.com.br'),
       ('2024-03-10', 8500.75, 'BRL', '11222335000170', 'pedro.costa@credits.com.br');

5. Auditoria:
   INSERT INTO auditoria.historico_execucao (...)
   VALUES (
       script_nome='ingest_faturamentos.py',
       tabela_destino='bronze.faturamentos',
       status='sucesso',
       linhas_processadas=3,
       linhas_inseridas=3,
       linhas_rejeitadas=0
   );
```

**Passo 2: Transformação Silver (Fato Faturamento)**

```python
# python/transformers/silver/transform_fato_faturamento.py::executar()

1. Extração Bronze:
   SELECT * FROM bronze.faturamentos;

   df:
   | data       | receita  | moeda | cnpj_cliente   | email_usuario
   |------------|----------|-------|----------------|-------------------
   | 2024-01-15 | 15000.50 | BRL   | 11222333000181 | joao.silva@...
   | 2024-02-20 | 25000.00 | BRL   | 11222334000126 | maria.santos@...
   | 2024-03-10 | 8500.75  | BRL   | 11222335000170 | pedro.costa@...

2. Transformações:
   - Calcular valor_liquido = valor_bruto (sem desconto)
   - Calcular valor_imposto = 15000.50 * 0.15 = 2250.08
   - Calcular valor_comissao = 15000.50 * 0.05 = 750.03
   - Calcular hash_transacao = MD5('11222333000181_joao.silva@credits.com.br_2024-01-15_15000.50')
     → '31673294794c2143118898dc9293cdd2'

3. Resolução de FKs:

   a) cliente_sk:
      SELECT cliente_sk, cnpj_cpf_nk
      FROM silver.dim_cliente
      WHERE flag_ativo = true;

      JOIN com bronze.faturamentos.cnpj_cliente = dim_cliente.cnpj_cpf_nk
      → cliente_sk = 31

   b) usuario_sk:
      SELECT usuario_sk, usuario_nk
      FROM silver.dim_usuario
      WHERE flag_ativo = true;

      JOIN com bronze.faturamentos.email_usuario = dim_usuario.usuario_nk
      → usuario_sk = 22

   c) data_sk:
      SELECT data_sk, data_completa
      FROM silver.dim_data;

      JOIN com bronze.faturamentos.data = dim_data.data_completa
      → data_sk = 1 (para 2024-01-15)

4. Validação de Qualidade:
   - FKs não-nulas: ✅ 0 NULLs
   - valor_bruto > 0: ✅ TODAS positivas
   - hash_transacao único: ✅ ÚNICO

5. Carga Final:
   TRUNCATE TABLE silver.fato_faturamento;
   INSERT INTO silver.fato_faturamento (
       cliente_sk, usuario_sk, data_sk,
       valor_bruto, valor_liquido, valor_imposto, valor_comissao,
       moeda, hash_transacao
   ) VALUES (
       31, 22, 1,
       15000.50, 15000.50, 2250.08, 750.03,
       'BRL', '31673294794c2143118898dc9293cdd2'
   );
```

**Passo 3: Agregação Gold (vendas_diarias)**

```sql
-- SQL View auto-atualiza ao consultar
SELECT * FROM gold.vendas_diarias WHERE data_completa = '2024-01-15';

-- Resultado:
data_completa | consultor   | total_transacoes | receita_bruta | receita_liquida | comissao_total | moeda
--------------|-------------|------------------|---------------|-----------------|----------------|-------
2024-01-15    | João Silva  | 1                | 15000.50      | 15000.50        | 750.03         | BRL

-- Internamente, a view executa:
SELECT
    d.data_completa,
    u.nome_completo AS consultor,
    COUNT(f.faturamento_sk) AS total_transacoes,
    SUM(f.valor_bruto) AS receita_bruta,
    SUM(f.valor_liquido) AS receita_liquida,
    SUM(f.valor_comissao) AS comissao_total,
    f.moeda
FROM silver.fato_faturamento f
INNER JOIN silver.dim_data d ON d.data_sk = f.data_sk
INNER JOIN silver.dim_usuario u ON u.usuario_sk = f.usuario_sk AND u.flag_ativo = true
WHERE d.data_completa = '2024-01-15'
GROUP BY d.data_completa, u.nome_completo, f.moeda;
```

### 7.2 Cenário Real: Mudança de Status do Cliente (SCD Type 2)

**Estado Inicial:**
```sql
SELECT cliente_sk, cnpj_cpf_nk, razao_social, status, versao, flag_ativo, data_inicio, data_fim
FROM silver.dim_cliente
WHERE cnpj_cpf_nk = '11222333000181';

-- Resultado:
cliente_sk | cnpj_cpf_nk    | razao_social      | status | versao | flag_ativo | data_inicio | data_fim
-----------|----------------|-------------------|--------|--------|------------|-------------|----------
31         | 11222333000181 | Alpha Tecnologia  | ATIVO  | 1      | true       | 2023-02-01  | NULL
```

**Mudança no Bronze:** `contas.csv` é atualizado
```csv
CNPJ/CPF;Tipo;Status;Data Criação;Razão Social
11222333000181;PJ;SUSPENSO;2023-02-01;Alpha Tecnologia Ltda
```

**Próxima Execução Silver:**

```python
# python/transformers/silver/transform_dim_cliente.py::processar_scd2()

1. Extração Bronze:
   df_bronze: status = 'SUSPENSO'

2. Extração Silver Atual:
   df_silver_atual: status = 'ATIVO' (flag_ativo=true)

3. Cálculo de Hash:
   hash_novo = MD5('11222333000181_SUSPENSO_Alpha Tecnologia_...')
             = 'abc123...'

   hash_atual = MD5('11222333000181_ATIVO_Alpha Tecnologia_...')
              = 'def456...'

   hash_novo != hash_atual → MUDANÇA DETECTADA

4. Ação SCD Type 2:

   a) Desativar versão antiga:
      UPDATE silver.dim_cliente
      SET flag_ativo = false,
          data_fim = '2025-11-27'
      WHERE cliente_sk = 31;

   b) Inserir nova versão:
      INSERT INTO silver.dim_cliente (
          cnpj_cpf_nk, razao_social, status, versao, flag_ativo, data_inicio, motivo_mudanca, hash_registro
      ) VALUES (
          '11222333000181', 'Alpha Tecnologia', 'SUSPENSO', 2, true, '2025-11-27',
          'Alteração de status: ATIVO → SUSPENSO', 'abc123...'
      );
      → cliente_sk = 41 (novo surrogate key)

5. Resultado Final:
   SELECT cliente_sk, cnpj_cpf_nk, status, versao, flag_ativo, data_inicio, data_fim
   FROM silver.dim_cliente
   WHERE cnpj_cpf_nk = '11222333000181'
   ORDER BY versao;

   cliente_sk | cnpj_cpf_nk    | status    | versao | flag_ativo | data_inicio | data_fim
   -----------|----------------|-----------|--------|------------|-------------|----------
   31         | 11222333000181 | ATIVO     | 1      | false      | 2023-02-01  | 2025-11-27  (histórico)
   41         | 11222333000181 | SUSPENSO  | 2      | true       | 2025-11-27  | NULL        (ativo)
```

**Impacto no Fato:**
```sql
-- Transações antigas continuam apontando para cliente_sk=31 (versão histórica)
-- Próximas transações apontarão para cliente_sk=41 (versão atual)

SELECT
    f.faturamento_sk,
    f.cliente_sk,
    c.razao_social,
    c.status,
    c.versao,
    d.data_completa
FROM silver.fato_faturamento f
JOIN silver.dim_cliente c ON c.cliente_sk = f.cliente_sk
JOIN silver.dim_data d ON d.data_sk = f.data_sk
WHERE c.cnpj_cpf_nk = '11222333000181';

-- Resultado:
faturamento_sk | cliente_sk | razao_social      | status | versao | data_completa
---------------|------------|-------------------|--------|--------|---------------
53             | 31         | Alpha Tecnologia  | ATIVO  | 1      | 2024-01-15

-- Interpretação: Na data 2024-01-15, o cliente estava ATIVO (versão 1)
-- Se houver nova transação após 2025-11-27, usará cliente_sk=41 (SUSPENSO, versão 2)
```

---

## 8. CLASSES E MÉTODOS PRINCIPAIS

### 8.1 BaseCSVIngestor (Bronze)

**Arquivo:** `python/ingestors/csv/base_csv_ingestor.py`

**Métodos Públicos:**

#### `executar() -> None`
```python
def executar(self) -> None:
    """
    Método principal (Template Method pattern).
    Orquestra todo o fluxo de ingestão Bronze.

    Fluxo:
    1. Valida arquivo existe
    2. Conecta ao banco
    3. Registra auditoria (em_execucao)
    4. Lê CSV
    5. Valida linhas (rigoroso)
    6. Transforma para Bronze
    7. Insere no banco (TRUNCATE/RELOAD)
    8. Arquiva arquivo
    9. Finaliza auditoria (sucesso/erro)

    Raises:
        FileNotFoundError: Se arquivo CSV não existe
        ValueError: Se validação falhar
        psycopg2.Error: Se erro de banco
    """
```

**Métodos Abstratos (implementar em child classes):**

#### `get_column_mapping() -> Dict[str, str]`
```python
@abstractmethod
def get_column_mapping(self) -> Dict[str, str]:
    """
    Retorna mapeamento de colunas CSV → Bronze.

    Returns:
        Dict[str, str]: {
            'Coluna CSV': 'coluna_bronze',
            ...
        }

    Exemplo:
        {
            'Data': 'data',
            'Receita': 'receita',
            'CNPJ Cliente': 'cnpj_cliente'
        }
    """
    pass
```

#### `get_bronze_columns() -> List[str]`
```python
@abstractmethod
def get_bronze_columns(self) -> List[str]:
    """
    Retorna lista de colunas da tabela Bronze (em ordem).

    Returns:
        List[str]: ['coluna1', 'coluna2', ...]

    Exemplo:
        ['data', 'receita', 'moeda', 'cnpj_cliente', 'email_usuario']
    """
    pass
```

#### `get_validation_rules() -> Dict[str, dict]`
```python
@abstractmethod
def get_validation_rules(self) -> Dict[str, dict]:
    """
    Retorna regras de validação para cada campo.

    Returns:
        Dict[str, dict]: {
            'nome_campo': {
                'obrigatorio': bool,
                'tipo': str,  # 'string', 'int', 'float', 'data', 'email', 'cnpj_cpf'
                'dominio': List[str],  # valores permitidos
                'positivo': bool,
                'min_len': int,
                'max_len': int,
                ...
            },
            ...
        }

    Exemplo:
        {
            'receita': {
                'obrigatorio': True,
                'tipo': 'decimal',
                'positivo': True
            },
            'moeda': {
                'obrigatorio': True,
                'tipo': 'string',
                'dominio': ['BRL', 'USD', 'EUR']
            }
        }
    """
    pass
```

**Métodos Privados (uso interno):**

#### `_validar_linha(row: pd.Series) -> Tuple[bool, Dict]`
```python
def _validar_linha(self, row: pd.Series) -> Tuple[bool, Dict]:
    """
    Valida uma linha do CSV contra as regras definidas.

    Args:
        row: Linha do DataFrame pandas

    Returns:
        (True, {}) se válido
        (False, {'campo': 'motivo_erro', ...}) se inválido

    Exemplo:
        row = {'receita': '-100.50', 'moeda': 'XXX'}
        → (False, {
            'receita': 'Valor deve ser positivo',
            'moeda': 'Valor XXX não está no domínio permitido: BRL, USD, EUR'
        })
    """
```

### 8.2 BaseSilverTransformer (Silver)

**Arquivo:** `python/transformers/silver/base_silver_transformer.py`

**Métodos Públicos:**

#### `executar() -> None`
```python
def executar(self) -> None:
    """
    Método principal (Template Method pattern).
    Orquestra todo o fluxo de transformação Silver.

    Fluxo:
    1. Conecta ao banco
    2. Registra auditoria (em_execucao)
    3. Extrai dados do Bronze
    4. Aplica transformações de negócio
    5. Valida qualidade
    6. Processa SCD Type 2 (dimensões) ou TRUNCATE/RELOAD (fatos)
    7. Finaliza auditoria (sucesso/erro)

    Raises:
        ValueError: Se validação de qualidade falhar
        psycopg2.Error: Se erro de banco
    """
```

**Métodos Abstratos (implementar em child classes):**

#### `extrair_bronze(conn) -> pd.DataFrame`
```python
@abstractmethod
def extrair_bronze(self, conn) -> pd.DataFrame:
    """
    Extrai dados da camada Bronze.

    Args:
        conn: Conexão PostgreSQL (psycopg2)

    Returns:
        pd.DataFrame: Dados extraídos do Bronze

    Exemplo:
        df = pd.read_sql("SELECT * FROM bronze.faturamentos", conn)
        return df
    """
    pass
```

#### `aplicar_transformacoes(df: pd.DataFrame) -> pd.DataFrame`
```python
@abstractmethod
def aplicar_transformacoes(self, df: pd.DataFrame) -> pd.DataFrame:
    """
    Aplica transformações de negócio nos dados.

    Args:
        df: DataFrame com dados do Bronze

    Returns:
        pd.DataFrame: Dados transformados para Silver

    Transformações típicas:
    - Padronização (CNPJ formatado, emails lowercase)
    - Derivações (tempo_cliente_dias, nivel_hierarquia)
    - Cálculos (valor_liquido, valor_imposto)
    - Renomeação de colunas

    Exemplo:
        df['cnpj_cpf_formatado'] = df['cnpj_cpf_nk'].apply(formatar_cnpj)
        df['tempo_cliente_dias'] = (pd.Timestamp.now() - df['data_criacao']).dt.days
        return df
    """
    pass
```

#### `validar_qualidade(df: pd.DataFrame) -> Tuple[bool, List[str]]`
```python
@abstractmethod
def validar_qualidade(self, df: pd.DataFrame) -> Tuple[bool, List[str]]:
    """
    Valida qualidade dos dados transformados.

    Args:
        df: DataFrame transformado

    Returns:
        (True, []) se válido
        (False, ['erro1', 'erro2', ...]) se inválido

    Validações típicas:
    - Campos NOT NULL sem NULLs
    - Tipos de dados corretos
    - Foreign Keys resolvidas (sem NULLs)
    - Valores numéricos em ranges válidos

    Exemplo:
        erros = []
        if df['cliente_sk'].isna().any():
            erros.append('Existem FKs de cliente não resolvidas')
        if (df['valor_bruto'] <= 0).any():
            erros.append('Existem valores brutos <= 0')
        return (len(erros) == 0, erros)
    """
    pass
```

**Métodos Fornecidos (uso direto):**

#### `processar_scd2(df: pd.DataFrame, conn) -> None`
```python
def processar_scd2(self, df: pd.DataFrame, conn) -> None:
    """
    Processa SCD Type 2 para dimensões.

    Algoritmo:
    1. Calcula hash_registro (MD5) para cada linha nova
    2. Busca registros ativos atuais (flag_ativo=true)
    3. Compara hash novo vs atual:
       - NOVOS → INSERT (versao=1, flag_ativo=true)
       - ALTERADOS →
         - UPDATE old (flag_ativo=false, data_fim=TODAY)
         - INSERT new (versao+=1, flag_ativo=true)
       - INALTERADOS → SKIP
    4. Executa UPDATEs e INSERTs transacionalmente

    Args:
        df: DataFrame transformado
        conn: Conexão PostgreSQL

    Raises:
        psycopg2.Error: Se erro de banco
    """
```

#### `calcular_hash_registro(df: pd.DataFrame, campos: List[str]) -> pd.Series`
```python
def calcular_hash_registro(self, df: pd.DataFrame, campos: List[str]) -> pd.Series:
    """
    Calcula hash MD5 para detectar mudanças em registros.

    Args:
        df: DataFrame
        campos: Lista de colunas para incluir no hash

    Returns:
        pd.Series: Hash MD5 para cada linha

    Exemplo:
        df['hash_registro'] = self.calcular_hash_registro(
            df,
            ['cnpj_cpf_nk', 'razao_social', 'status', 'tipo_pessoa']
        )
    """
```

### 8.3 Validadores (utils/validators.py)

#### `validar_cnpj_cpf(valor: str) -> Tuple[bool, str]`
```python
def validar_cnpj_cpf(valor: str) -> Tuple[bool, str]:
    """
    Valida CNPJ (14 dígitos) ou CPF (11 dígitos) com dígitos verificadores.

    Algoritmo:
    1. Remove caracteres não-numéricos
    2. Valida tamanho (11 para CPF, 14 para CNPJ)
    3. Rejeita dígitos repetidos (11111111111111)
    4. Calcula dígito verificador 1 (algoritmo oficial)
    5. Calcula dígito verificador 2 (algoritmo oficial)
    6. Compara com dígitos recebidos

    Args:
        valor: CNPJ/CPF (com ou sem formatação)

    Returns:
        (True, "") se válido
        (False, "mensagem de erro") se inválido

    Exemplos:
        validar_cnpj_cpf('11.222.333/0001-81') → (True, "")
        validar_cnpj_cpf('12345678000195') → (False, "CNPJ inválido (dígito verificador incorreto)")
        validar_cnpj_cpf('123') → (False, "CNPJ/CPF deve ter 11 ou 14 dígitos, recebido: 3")
    """
```

#### `validar_email(valor: str) -> Tuple[bool, str]`
```python
def validar_email(valor: str) -> Tuple[bool, str]:
    """
    Valida formato de email usando regex.

    Regex: ^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$

    Args:
        valor: Email a validar

    Returns:
        (True, "") se válido
        (False, "Email inválido: <valor>") se inválido

    Exemplos:
        validar_email('teste@example.com') → (True, "")
        validar_email('teste@') → (False, "Email inválido: teste@")
    """
```

#### `validar_valor_dominio(valor: str, dominio: List[str], case_sensitive: bool = True) -> Tuple[bool, str]`
```python
def validar_valor_dominio(
    valor: str,
    dominio: List[str],
    case_sensitive: bool = True
) -> Tuple[bool, str]:
    """
    Valida se valor está em lista de valores permitidos.

    Args:
        valor: Valor a validar
        dominio: Lista de valores permitidos
        case_sensitive: Se True, diferencia maiúsculas/minúsculas

    Returns:
        (True, "") se válido
        (False, "Valor '<valor>' não está no domínio permitido: ...") se inválido

    Exemplos:
        validar_valor_dominio('BRL', ['BRL', 'USD', 'EUR']) → (True, "")
        validar_valor_dominio('XXX', ['BRL', 'USD', 'EUR'])
            → (False, "Valor 'XXX' não está no domínio permitido: BRL, USD, EUR")
        validar_valor_dominio('brl', ['BRL', 'USD', 'EUR'], case_sensitive=False) → (True, "")
    """
```

---

## 9. SISTEMA DE AUDITORIA E LOGS

### 9.1 Tabela de Auditoria

**Tabela:** `auditoria.historico_execucao`

```sql
CREATE TABLE auditoria.historico_execucao (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    script_nome VARCHAR(255) NOT NULL,
    tabela_destino VARCHAR(100),
    status VARCHAR(20) NOT NULL,          -- 'em_execucao', 'sucesso', 'erro'
    data_inicio TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    data_fim TIMESTAMP WITH TIME ZONE,
    linhas_processadas INTEGER,
    linhas_inseridas INTEGER,
    linhas_rejeitadas INTEGER,
    mensagem_erro TEXT,
    execucao_duracao_ms INTEGER,
    usuario_execucao VARCHAR(100),
    ambiente VARCHAR(20)                   -- 'production', 'staging', 'dev'
);

CREATE INDEX idx_historico_script ON auditoria.historico_execucao(script_nome);
CREATE INDEX idx_historico_status ON auditoria.historico_execucao(status);
CREATE INDEX idx_historico_data ON auditoria.historico_execucao(data_inicio DESC);
```

**Dados Reais (amostra):**
```
id                                   | script_nome              | status  | linhas_processadas | linhas_inseridas | data_inicio
-------------------------------------|--------------------------|---------|-------------------|------------------|---------------------
a1b2c3d4-5678-90ab-cdef-123456789abc | ingest_faturamentos.py   | sucesso | 13                | 13               | 2025-11-27 11:00:15
b2c3d4e5-6789-01bc-def0-234567890bcd | transform_dim_cliente.py | sucesso | 10                | 10               | 2025-11-27 11:02:15
```

**Queries Úteis:**

#### Histórico de execuções (últimas 24h)
```sql
SELECT
    script_nome,
    tabela_destino,
    status,
    data_inicio,
    linhas_processadas,
    linhas_inseridas,
    linhas_rejeitadas,
    execucao_duracao_ms / 1000.0 AS duracao_segundos
FROM auditoria.historico_execucao
WHERE data_inicio >= NOW() - INTERVAL '24 hours'
ORDER BY data_inicio DESC;
```

#### Taxa de sucesso por script (últimos 30 dias)
```sql
SELECT
    script_nome,
    COUNT(*) AS total_execucoes,
    COUNT(*) FILTER (WHERE status = 'sucesso') AS sucessos,
    COUNT(*) FILTER (WHERE status = 'erro') AS erros,
    ROUND(
        COUNT(*) FILTER (WHERE status = 'sucesso')::numeric / COUNT(*) * 100,
        2
    ) AS taxa_sucesso_pct
FROM auditoria.historico_execucao
WHERE data_inicio >= NOW() - INTERVAL '30 days'
GROUP BY script_nome
ORDER BY taxa_sucesso_pct ASC;
```

#### Performance média por script
```sql
SELECT
    script_nome,
    COUNT(*) AS total_execucoes,
    AVG(execucao_duracao_ms) / 1000.0 AS duracao_media_segundos,
    AVG(linhas_processadas) AS linhas_processadas_media,
    AVG(linhas_inseridas) AS linhas_inseridas_media
FROM auditoria.historico_execucao
WHERE status = 'sucesso'
  AND data_inicio >= NOW() - INTERVAL '30 days'
GROUP BY script_nome
ORDER BY duracao_media_segundos DESC;
```

### 9.2 Logs de Rejeição

**Tabela:** `auditoria.log_rejeicao`

```sql
CREATE TABLE auditoria.log_rejeicao (
    id BIGSERIAL PRIMARY KEY,
    execucao_id UUID NOT NULL,
    script_nome VARCHAR(255) NOT NULL,
    tabela_destino VARCHAR(100),
    numero_linha INTEGER,
    campo_falha VARCHAR(100),
    motivo_rejeicao TEXT NOT NULL,
    valor_recebido TEXT,
    registro_completo JSONB,
    severidade VARCHAR(20),               -- 'WARNING', 'ERROR', 'CRITICAL'
    data_rejeicao TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,

    CONSTRAINT fk_log_rejeicao_execucao FOREIGN KEY (execucao_id)
        REFERENCES auditoria.historico_execucao(id)
);

CREATE INDEX idx_log_rejeicao_execucao ON auditoria.log_rejeicao(execucao_id);
CREATE INDEX idx_log_rejeicao_campo ON auditoria.log_rejeicao(campo_falha);
```

**Queries Úteis:**

#### Top 10 campos mais rejeitados (últimos 30 dias)
```sql
SELECT
    campo_falha,
    COUNT(*) AS total_rejeicoes,
    COUNT(DISTINCT numero_linha) AS linhas_unicas,
    ARRAY_AGG(DISTINCT motivo_rejeicao) AS motivos
FROM auditoria.log_rejeicao
WHERE data_rejeicao >= NOW() - INTERVAL '30 days'
GROUP BY campo_falha
ORDER BY total_rejeicoes DESC
LIMIT 10;
```

#### Detalhamento de rejeições de uma execução específica
```sql
SELECT
    numero_linha,
    campo_falha,
    motivo_rejeicao,
    valor_recebido,
    registro_completo
FROM auditoria.log_rejeicao
WHERE execucao_id = 'a1b2c3d4-5678-90ab-cdef-123456789abc'
ORDER BY numero_linha;
```

#### Taxa de rejeição por script (últimos 30 dias)
```sql
SELECT
    h.script_nome,
    SUM(h.linhas_processadas) AS total_linhas,
    SUM(h.linhas_rejeitadas) AS total_rejeitadas,
    ROUND(
        SUM(h.linhas_rejeitadas)::numeric / NULLIF(SUM(h.linhas_processadas), 0) * 100,
        2
    ) AS taxa_rejeicao_pct
FROM auditoria.historico_execucao h
WHERE h.data_inicio >= NOW() - INTERVAL '30 days'
  AND h.script_nome LIKE 'ingest_%'
GROUP BY h.script_nome
ORDER BY taxa_rejeicao_pct DESC;
```

### 9.3 Logs de Aplicação

**Diretório:** `/home/brunodev/credits-dw/logs/`

**Arquivos:**
- `ingest_contas.log`
- `ingest_usuarios.log`
- `ingest_faturamentos.log`
- `transform_dim_cliente.log`
- `transform_dim_usuario.log`
- `transform_fato_faturamento.log`

**Formato de Log:**
```
[TIMESTAMP] [LEVEL] [SCRIPT] Mensagem

Exemplo:
[2025-11-27 11:00:15] [INFO] [ingest_faturamentos.py] Iniciando ingestão: bronze.faturamentos
[2025-11-27 11:00:15] [INFO] Arquivo encontrado: /app/data/input/onedrive/faturamentos.csv
[2025-11-27 11:00:15] [INFO] CSV lido: 13 linhas
[2025-11-27 11:00:16] [INFO] Validação: 13 válidas, 0 rejeitadas
[2025-11-27 11:00:16] [INFO] Inseridos 13 registros em bronze.faturamentos
[2025-11-27 11:00:16] [INFO] [SUCESSO] Ingestão concluída em 1.2s
```

**Configuração (logger.py):**
```python
from loguru import logger

logger.add(
    f"/app/logs/{script_name}.log",
    rotation="10 MB",      # Rotação a cada 10 MB
    retention="30 days",   # Manter logs por 30 dias
    compression="zip",     # Comprimir logs antigos
    level="INFO",
    format="{time:YYYY-MM-DD HH:mm:ss} | {level} | {message}"
)
```

---

## 10. TROUBLESHOOTING E FAQ

### 10.1 Problemas Comuns

#### **Erro: "psycopg2.OperationalError: connection to server failed"**

**Causa:** Credenciais de banco incorretas ou rede inacessível.

**Solução:**
1. Verificar `.env` tem credenciais corretas
2. Testar conexão manual:
```bash
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "SELECT 1"
```
3. Verificar firewall do Azure permite IP do Docker

#### **Erro: "FileNotFoundError: [Errno 2] No such file or directory: '<arquivo>.csv'"**

**Causa:** Arquivo CSV não está no diretório esperado.

**Solução:**
1. Verificar arquivo existe:
```bash
ls -l docker/data/input/onedrive/
```
2. Verificar nome do arquivo corresponde ao esperado pelo ingestor
3. Verificar permissões de leitura:
```bash
chmod 644 docker/data/input/onedrive/*.csv
```

#### **Erro: "IntegrityError: duplicate key value violates unique constraint"**

**Causa:** Tentativa de inserir registro duplicado (violação de PK ou UNIQUE).

**Solução:**
1. Para Bronze: Normal, pois usa TRUNCATE/RELOAD
2. Para Silver:
   - Dimensões: SCD Type 2 deve prevenir (verificar hash_registro)
   - Fatos: Verificar hash_transacao está calculado corretamente
3. Query para identificar duplicatas:
```sql
SELECT cnpj_cpf_nk, versao, COUNT(*)
FROM silver.dim_cliente
GROUP BY cnpj_cpf_nk, versao
HAVING COUNT(*) > 1;
```

#### **Erro: "ValueError: Existem FKs de cliente não resolvidas"**

**Causa:** Fato não conseguiu resolver FK para dimensão (cliente não existe em dim_cliente).

**Solução:**
1. Verificar cliente existe no Bronze:
```sql
SELECT * FROM bronze.contas WHERE cnpj_cpf = '<cnpj_problematico>';
```
2. Verificar cliente foi carregado na Silver:
```sql
SELECT * FROM silver.dim_cliente WHERE cnpj_cpf_nk = '<cnpj_problematico>' AND flag_ativo = true;
```
3. Se não existe: Executar `transform_dim_cliente.py` primeiro
4. Verificar ordem de execução: Dimensões ANTES de Fatos

#### **Erro: "UnicodeDecodeError: 'utf-8' codec can't decode byte"**

**Causa:** Arquivo CSV tem encoding diferente de UTF-8.

**Solução:**
1. `BaseCSVIngestor` já tenta fallback para ISO-8859-1
2. Se ainda falhar, converter arquivo:
```bash
iconv -f ISO-8859-1 -t UTF-8 arquivo_original.csv > arquivo_utf8.csv
```
3. Ou adicionar encoding no read_csv:
```python
df = pd.read_csv(file_path, sep=';', dtype=str, encoding='latin1')
```

### 10.2 FAQ

#### **P: Como adicionar um novo campo em uma dimensão existente?**

**R:**
1. Adicionar coluna na tabela Silver:
```sql
ALTER TABLE silver.dim_cliente ADD COLUMN novo_campo VARCHAR(100);
```
2. Atualizar `transform_dim_cliente.py::aplicar_transformacoes()`:
```python
df['novo_campo'] = df['coluna_bronze'].apply(lambda x: transformar(x))
```
3. Atualizar `calcular_hash_registro()` para incluir novo campo:
```python
hash_campos = ['cnpj_cpf_nk', 'razao_social', 'status', 'novo_campo']
```
4. Executar transformador: próxima execução criará novas versões

#### **P: Como deletar dados antigos das tabelas de auditoria?**

**R:**
```sql
-- Deletar logs de execução com mais de 90 dias
DELETE FROM auditoria.historico_execucao
WHERE data_inicio < NOW() - INTERVAL '90 days';

-- Deletar logs de rejeição órfãos (execução já foi deletada)
DELETE FROM auditoria.log_rejeicao
WHERE execucao_id NOT IN (SELECT id FROM auditoria.historico_execucao);
```

**Automação (cronjob):**
```sql
-- Criar função de limpeza
CREATE OR REPLACE FUNCTION auditoria.limpar_logs_antigos(dias INTEGER DEFAULT 90)
RETURNS INTEGER AS $$
DECLARE
    linhas_deletadas INTEGER;
BEGIN
    DELETE FROM auditoria.historico_execucao
    WHERE data_inicio < NOW() - (dias || ' days')::INTERVAL;

    GET DIAGNOSTICS linhas_deletadas = ROW_COUNT;
    RETURN linhas_deletadas;
END;
$$ LANGUAGE plpgsql;

-- Executar manualmente
SELECT auditoria.limpar_logs_antigos(90);
```

#### **P: Como reprocessar um CSV rejeitado após corrigir os dados?**

**R:**
1. Corrigir arquivo CSV
2. Mover de `/processed/` de volta para `/input/onedrive/`:
```bash
mv docker/data/processed/faturamentos_20251127_110016.csv docker/data/input/onedrive/faturamentos.csv
```
3. Executar ingestor novamente:
```bash
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamentos.py
```

#### **P: Como fazer backup do banco de dados?**

**R:**
```bash
# Backup completo
PGPASSWORD='58230925AD@' pg_dump \
    -h creditsdw.postgres.database.azure.com \
    -U creditsdw \
    -d creditsdw \
    -F c \
    -f backup_creditsdw_$(date +%Y%m%d).dump

# Backup apenas schema (sem dados)
PGPASSWORD='58230925AD@' pg_dump \
    -h creditsdw.postgres.database.azure.com \
    -U creditsdw \
    -d creditsdw \
    --schema-only \
    -f schema_creditsdw_$(date +%Y%m%d).sql

# Restaurar backup
PGPASSWORD='58230925AD@' pg_restore \
    -h creditsdw.postgres.database.azure.com \
    -U creditsdw \
    -d creditsdw \
    backup_creditsdw_20251127.dump
```

#### **P: Como otimizar performance das queries Gold?**

**R:**
1. Criar índices nas dimensões (já implementados):
```sql
CREATE INDEX idx_dim_cliente_ativo ON silver.dim_cliente(flag_ativo) WHERE flag_ativo = true;
CREATE INDEX idx_dim_data_ano_mes ON silver.dim_data(ano, mes);
```
2. Criar índices nas FKs do fato (se queries lentas):
```sql
CREATE INDEX idx_fato_faturamento_cliente ON silver.fato_faturamento(cliente_sk);
CREATE INDEX idx_fato_faturamento_usuario ON silver.fato_faturamento(usuario_sk);
CREATE INDEX idx_fato_faturamento_data ON silver.fato_faturamento(data_sk);
```
3. Materializar views Gold (se volume > 100K linhas):
```sql
CREATE MATERIALIZED VIEW gold.vendas_mensais_mat AS
SELECT * FROM gold.vendas_mensais;

-- Atualizar periodicamente (cronjob)
REFRESH MATERIALIZED VIEW gold.vendas_mensais_mat;
```

#### **P: Como adicionar validação customizada?**

**R:**
1. Criar função validadora em `utils/validators.py`:
```python
def validar_telefone(valor: str) -> Tuple[bool, str]:
    """Valida formato de telefone brasileiro"""
    pattern = r'^\(\d{2}\)\s?\d{4,5}-\d{4}$'
    if re.match(pattern, str(valor)):
        return True, ""
    return False, f"Telefone inválido: {valor}"
```
2. Adicionar em `get_validation_rules()`:
```python
'telefone': {
    'obrigatorio': False,
    'tipo': 'custom',
    'validador': validar_telefone  # função custom
}
```
3. Atualizar `_validar_linha()` para suportar 'custom' type

---

## CONCLUSÃO

Este documento fornece uma visão completa e detalhada do Data Warehouse Credits Brasil, incluindo:

- ✅ **Arquitetura Medallion** (Bronze → Silver → Gold)
- ✅ **Fluxos de dados** com exemplos reais
- ✅ **Estrutura de tabelas** e relacionamentos
- ✅ **Classes e métodos** principais
- ✅ **Sistema de validação** rigoroso
- ✅ **Auditoria e logs** completos
- ✅ **Troubleshooting** e FAQ

**Dados Atualizados (27/11/2025):**
- Bronze: 401 registros validados
- Silver: 354 registros (Star Schema)
- Gold: 63 registros agregados
- Integridade: 100% (R$ 246.803,25 em todas camadas)

**Próximos Passos Recomendados:**
1. Implementar testes automatizados (pytest)
2. Adicionar CI/CD (GitHub Actions)
3. Criar dashboards Power BI
4. Expandir Gold layer com novas métricas de negócio
5. Implementar dim_canal (quando fonte de dados estiver completa)

**Manutenção:**
- Atualizar este documento a cada refatoração significativa
- Versionar junto com o código (git commit)
- Revisar FAQ trimestralmente

---

**Documento criado em:** 27/11/2025
**Última atualização:** 27/11/2025
**Versão:** 5.0
**Próxima revisão:** Trimestral ou após mudanças estruturais
