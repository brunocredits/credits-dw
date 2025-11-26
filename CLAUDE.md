# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Data Warehouse ETL pipeline** for Credits Brasil that ingests CSV files into a PostgreSQL database using a **Medallion architecture (Bronze/Silver)**. The project uses Python scripts orchestrated with Docker to process financial data from various sources.

**Version:** 2.0 (November 2025 - Major refactoring with rigorous validation)

**Key Architecture Concepts:**
- **Bronze Layer (v2.0 - RIGOROUS)**: Raw data storage with **strict validation**. Only 100% valid records enter the database. Invalid records are REJECTED and logged. Uses **TRUNCATE/RELOAD** strategy.
- **Silver Layer**: Dimensional model (Star Schema) with business logic, data quality rules, and SCD Type 2 for historical tracking. Uses **incremental/SCD Type 2** strategy.
- **Schemas**: `bronze` (validated raw data), `silver` (dimensional model), and `credits` (ETL metadata/audit/rejection logs)
- **Date Dimension**: `bronze.data` feeds `silver.dim_tempo` - pre-calculated date dimension with business calendar attributes
- **Audit Trail**: All ETL executions tracked in `credits.historico_atualizacoes`, rejections in `credits.logs_rejeicao`, Silver loads in `credits.silver_control`

**Current Data Status:**
- Bronze: ✅ Operational with validation (4 tables: 6 contas, 6 usuarios, 9 faturamento, 4,018 datas)
- Silver: ✅ Fully loaded (dim_tempo: 4,018, dim_canal: 7, dim_clientes: 9, dim_usuarios: 5, fact_faturamento: 9)
- Rejection logs: Active and tracking invalid records

## Critical Architectural Principles

**1. Data Quality First (v2.0):**
- Bronze layer REJECTS invalid data - it does NOT accept everything
- All ingestors MUST implement `get_validation_rules()` method
- Invalid records are logged to `credits.logs_rejeicao` for auditing
- Only 100% validated data enters the database

**2. Template Method Pattern:**
- `BaseCSVIngestor` defines the execution flow (Template Method pattern)
- Child classes implement: `get_column_mapping()`, `get_bronze_columns()`, `get_validation_rules()`
- Never override `executar()` method - extend through template methods

**3. Security:**
- SQL injection protection via whitelist: only tables in `TABELAS_PERMITIDAS` are allowed
- Never construct SQL with string interpolation - use `psycopg2.sql` for identifiers
- Database credentials via environment variables, never hardcoded

**4. Idempotency:**
- Bronze: TRUNCATE/RELOAD (full replace each execution)
- Silver: SCD Type 2 for dimensions (detect changes via MD5 hash), FULL for facts
- All operations can be safely re-run

**5. Code Style:**
- Comments in Portuguese (comentários em português)
- Clean Code principles (funções pequenas e focadas)
- Type hints on all function signatures
- Comprehensive docstrings

## Common Commands

All commands should be run from the project root directory.

### Docker Operations

**IMPORTANTE:** Todos os comandos `docker compose` devem ser executados a partir do diretório `docker/`:

```bash
# Construir e iniciar o container ETL
cd docker && docker compose up -d --build

# Parar o ambiente
docker compose down

# Acessar shell do container para debugging
docker compose exec etl-processor bash

# Ou da raiz do projeto com contexto explícito:
docker compose -f docker/docker-compose.yml up -d --build
```

### Running ETL Scripts

**Bronze Layer (CSV Ingestors):**
```bash
# Execute ALL CSV ingestors at once
docker compose exec etl-processor python python/run_all_ingestors.py

# Execute a specific ingestor
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py
docker compose exec etl-processor python python/ingestors/csv/ingest_usuarios.py
docker compose exec etl-processor python python/ingestors/csv/ingest_contas_base_oficial.py
```

**Silver Layer (Transformers):**
```bash
# Execute ALL Silver transformations
docker compose exec etl-processor python python/run_silver_transformations.py

# Execute a specific transformer
docker compose exec etl-processor python python/transformers/silver/transform_dim_tempo.py
docker compose exec etl-processor python python/transformers/silver/transform_dim_clientes.py
```

### Code Quality

```bash
# Formatar código
black python/

# Analisar código (linter)
ruff check .

# Verificação de tipos
mypy python/
```

### Testes

```bash
# Executar todos os testes
pytest

# Executar testes com relatório de cobertura
pytest --cov=python --cov-report=html

# Executar arquivo de teste específico
pytest tests/test_example.py

# Executar testes em modo verbose
pytest -v

# Configuração em pytest.ini na raiz do projeto
```

## Architecture & Code Structure

### Ingestor Pattern (Template Method) - Version 2.0

All CSV ingestors inherit from `BaseCSVIngestor` in `python/ingestors/csv/base_csv_ingestor.py`, which implements the Template Method pattern with **rigorous validation**. When creating a new ingestor:

1. **Inherit from `BaseCSVIngestor`**
2. **Implement three required methods:**
   - `get_column_mapping()`: Returns dict mapping CSV columns to Bronze table columns
   - `get_bronze_columns()`: Returns list of Bronze table column names in order
   - `get_validation_rules()`: Returns dict with validation rules for each field (**NEW in v2.0**)

3. **Call super().__init__()** with:
   - `script_name`: Script filename for audit logs
   - `tabela_destino`: Full table name (e.g., `'bronze.faturamento'`)
   - `arquivo_nome`: CSV filename to process
   - `input_subdir`: Subdirectory under `/app/data/input/` (usually `'onedrive'`)

**Example ingestor structure** (`python/ingestors/csv/ingest_faturamento.py`):
```python
class IngestFaturamento(BaseCSVIngestor):
    def __init__(self):
        super().__init__(
            script_name='ingest_faturamento.py',
            tabela_destino='bronze.faturamento',
            arquivo_nome='faturamento.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        return {
            'Data': 'data',
            'Receita': 'receita',
            'Moeda': 'moeda',
            'CNPJ Cliente': 'cnpj_cliente',
            'Email Usuario': 'email_usuario'
        }

    def get_bronze_columns(self) -> List[str]:
        return ['data', 'receita', 'moeda', 'cnpj_cliente', 'email_usuario']

    def get_validation_rules(self) -> Dict[str, dict]:
        """Define validation rules for each field (REQUIRED in v2.0)"""
        return {
            'data': {
                'obrigatorio': True,
                'tipo': 'data',
                'formato_data': '%Y-%m-%d'
            },
            'receita': {
                'obrigatorio': True,
                'tipo': 'decimal',
                'positivo': True
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

**Available validation rules:**
- `obrigatorio`: True/False - field must have value
- `tipo`: 'string', 'int', 'float', 'decimal', 'data', 'email', 'cnpj_cpf'
- `min_len` / `max_len`: string length constraints
- `minimo` / `maximo`: numeric range constraints
- `positivo`: True - number must be > 0
- `nao_negativo`: True - number must be >= 0
- `dominio`: list of allowed values (e.g., ['BRL', 'USD', 'EUR'])
- `case_sensitive`: True/False - for domain validation
- `formato_data`: '%Y-%m-%d' - expected date format

### ETL Execution Flow (BaseCSVIngestor v2.0)

The `executar()` method orchestrates a **rigorous validation pipeline**:

1. File validation
2. Database connection (`utils.db_connection.get_db_connection()`)
3. Audit registration (`utils.audit.registrar_execucao()`)
4. CSV reading (`ler_csv()` - reads all as strings, handles encoding)
5. **Row-by-row validation** (`validar_linha()` - NEW in v2.0)
   - Validates each field against rules from `get_validation_rules()`
   - Rejects invalid rows immediately
   - Logs rejections to `credits.logs_rejeicao`
6. Bronze transformation (`transformar_para_bronze()` - applies column mapping to VALID rows only)
7. Database insertion (`inserir_bronze()` - **TRUNCATE/RELOAD strategy**, ONLY valid data)
8. Rejection logs saved to database
9. File archival (moves processed file to `docker/data/processed/` with timestamp)
10. Audit finalization (`utils.audit.finalizar_execucao()`)

**CRITICAL CHANGE in v2.0**: Bronze layer now **REJECTS invalid data** before insertion. Only 100% valid records enter the database. Invalid records are logged to `credits.logs_rejeicao` for auditing.

**IMPORTANT**: Bronze layer uses **TRUNCATE/RELOAD**, not incremental loads. Each execution completely replaces the table data.

### Database Connection

- Connection logic in `python/utils/db_connection.py`
- Credentials loaded from environment variables (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
- Two connection modes:
  - `get_db_connection()`: Direct connection (used by ingestors)
  - `get_pooled_connection()`: Connection pooling (available but not currently used)

**Quick database access (pre-approved commands):**
```bash
# View Bronze layer structure
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "\d bronze.faturamento"

# Query data directly
PGPASSWORD='58230925AD@' psql -h creditsdw.postgres.database.azure.com -U creditsdw -d creditsdw -c "SELECT * FROM bronze.faturamento LIMIT 5"

# Run Python scripts with environment
DB_HOST=creditsdw.postgres.database.azure.com DB_PORT=5432 DB_NAME=creditsdw DB_USER=creditsdw DB_PASSWORD='58230925AD@' python3 <script>
```

### Audit System

All ETL runs are tracked in `credits.historico_atualizacoes`:
- `registrar_execucao()`: Logs start with status `'em_execucao'`
- `finalizar_execucao()`: Updates with final status (`'sucesso'` or `'erro'`), line counts, and error messages

### File Paths (Inside Docker Container)

- **Input CSVs**: `/app/data/input/onedrive/`
- **Processed Archive**: `/app/data/processed/`
- **Templates**: `/app/data/templates/` (example CSV files with headers)
- **Logs**: `/app/logs/`

These are mounted from the host `docker/data/` directory.

### Date Handling

Columns starting with `data_` or `dt_` are automatically converted to `YYYY-MM-DD` format by `_formatar_colunas_data()` in `python/ingestors/csv/base_csv_ingestor.py:175-195`. Invalid dates are converted to `None`.

### Padrões Importantes de Código

**Context Managers para Conexões de Banco:**
```python
from utils.db_connection import get_connection

with get_connection() as conn:
    # Conexão fechada automaticamente após o bloco
    df = pd.read_sql("SELECT * FROM bronze.faturamento", conn)
```

**Boas Práticas de Logging:**
```python
from utils.logger import setup_logger

logger = setup_logger(__name__)
logger.info("Iniciando processo")
logger.warning("Encontrados 5 valores nulos")
logger.error("Falha na conexão", exc_info=True)
```

**Detecção Automática de Colunas de Data:**
Colunas nomeadas como `data_*` ou `dt_*` são automaticamente formatadas para `YYYY-MM-DD` pelo `BaseCSVIngestor`. Datas inválidas se tornam `None`.

## Bronze Layer Tables

**bronze.contas_base_oficial** (6 records):
- `cnpj_cpf`, `tipo`, `status`, `status_qualificação_da_conta`
- `data_criacao`, `grupo`, `razao_social`, `responsavel_conta` ✓ (typo corrigido de "resposanvel_conta")
- `financeiro`, `corte`, `faixa`, `sk_id` (PK)

**bronze.usuarios** (6 records):
- `nome_empresa`, `Nome`, `area`, `senioridade`, `gestor`, `email`
- `canal_1`, `canal_2` ✓ (renomeado de "canal 1", "canal 2")
- `email_lider`, `sk_id` (PK)

**bronze.faturamento** (9 records):
- `data`, `receita`, `moeda`, `cnpj_cliente`, `email_usuario`, `sk_id` (PK)

**bronze.data** (no PK required - reference table):
- `data_completa`, `ano`, `mes`, `dia`, `bimestre`, `trimestre`, `quarter`, `semestre`

## Adding a New CSV Ingestor (v2.0)

1. Create new file in `python/ingestors/csv/ingest_<name>.py`
2. Implement class inheriting from `BaseCSVIngestor`
3. Define column mappings, Bronze column list, AND validation rules (required in v2.0)
4. Add CSV file to `docker/data/input/onedrive/`
5. Create corresponding Bronze table in PostgreSQL
6. Create database migration for `credits.logs_rejeicao` if not exists
7. Add ingestor instance to `python/run_all_ingestors.py` if it should run with all ingestors

**Migration Required for v2.0:**
Ensure `credits.logs_rejeicao` table exists. See `RESUMO_REFATORACAO_BRONZE.md` for SQL schema.

## Silver Layer (Dimensional Model)

The Silver layer implements a **Star Schema** with dimensions and fact tables, applying business rules and data quality transformations.

### Silver Tables

**Dimensions:**
- `silver.dim_tempo` ✓ **(4,018 registros)**
  - PK: `sk_data` (integer)
  - Natural Key: `data_completa` (date, UNIQUE)
  - Atributos: ano, mes, dia, trimestre, semestre, nome_mes, dia_semana, flags (fim_semana, dia_util, feriado)

- `silver.dim_clientes` ✅ **(9 registros)**
  - PK: `sk_cliente` (integer)
  - Natural Key: `nk_cnpj_cpf` (varchar, com versioning)
  - SCD Type 2: `data_inicio`, `data_fim`, `flag_ativo`, `versao`, `hash_registro`, `motivo_mudanca`
  - UNIQUE: `uk_cliente_cnpj_versao` (nk_cnpj_cpf + versao)

- `silver.dim_usuarios` ✅ **(5 registros)**
  - PK: `sk_usuario` (integer)
  - Natural Key: `nk_usuario` (varchar, email limpo)
  - FK: `sk_gestor` → `dim_usuarios.sk_usuario` (hierarquia self-referencing)
  - SCD Type 2: `data_inicio`, `data_fim`, `flag_ativo`, `versao`, `hash_registro`

- `silver.dim_canal` ✅ **(7 registros)**
  - PK: `sk_canal` (integer)
  - Natural Key: `tipo_canal` + `nome_canal` (combinação única)
  - Atributos: categoria_canal, prioridade, comissao_percentual, meta_mensal

**Facts:**
- `silver.fact_faturamento` ✅ **(9 registros)**
  - PK: `sk_faturamento` (bigint)
  - FKs:
    - `sk_cliente` → `dim_clientes.sk_cliente`
    - `sk_usuario` → `dim_usuarios.sk_usuario`
    - `sk_data` → `dim_tempo.sk_data`
    - `sk_canal` → `dim_canal.sk_canal`
  - Measures: `valor_bruto`, `valor_liquido`, `valor_desconto`, `valor_imposto`, `valor_comissao`
  - Degenerate Dimensions: `numero_documento`, `tipo_documento`, `moeda`, `forma_pagamento`, `status_pagamento`
  - UNIQUE: `uk_fact_faturamento_hash` (hash_transacao para idempotência)

### Star Schema Diagram

```
                    ┌─────────────────┐
                    │  dim_tempo      │
                    │  PK: sk_data    │◄─────┐
                    │  UK: data_comp  │      │
                    └─────────────────┘      │
                                             │
    ┌─────────────────┐              ┌──────┴──────────────┐
    │  dim_clientes   │              │ fact_faturamento    │
    │  PK: sk_cliente │◄─────────────┤ PK: sk_faturamento  │
    │  UK: cnpj+ver   │              │ FK: sk_data         │
    │  SCD Type 2     │              │ FK: sk_cliente      │
    └─────────────────┘              │ FK: sk_usuario      │
                                     │ FK: sk_canal        │
    ┌─────────────────┐              │ UK: hash_transacao  │
    │  dim_usuarios   │              └──────┬──────────────┘
    │  PK: sk_usuario │◄─────────────────────┘
    │  FK: sk_gestor ─┼──┐                   │
    │  SCD Type 2     │  │                   │
    └─────────────────┘  │                   │
              ▲          │                   │
              └──────────┘                   │
           (hierarquia)                      │
                                     ┌───────▼──────────┐
                                     │  dim_canal       │
                                     │  PK: sk_canal    │
                                     │  UK: tipo+nome   │
                                     └──────────────────┘
```

### Transformer Pattern

All Silver transformers inherit from `BaseSilverTransformer` in `python/transformers/base_transformer.py`. Location: `python/transformers/silver/`

**Required methods (Template Method pattern):**
- `extrair_bronze(conn)`: Query Bronze tables and return DataFrame
- `aplicar_transformacoes(df)`: Apply business rules and data quality transformations
- `validar_qualidade(df)`: Validate data quality, return (success: bool, errors: List[str])

**Provided methods:**
- `executar()`: Main orchestration (extract → transform → validate → load)
- `processar_scd2()`: Handle SCD Type 2 logic (detect changes, version records)
- `calcular_hash_registro()`: Generate MD5 hash for change detection

**Execution:**
- Run all transformers: `python python/run_silver_transformations.py`
- Control table `credits.silver_control` tracks last execution timestamps and dependencies

**Implementation Status:**
- ✅ `BaseTransformer`: Fully implemented with SCD Type 2 support
- ✅ `transform_dim_tempo.py`: Fully implemented with calendar enrichment
- ✅ `transform_dim_canal.py`: Fully implemented
- ✅ `transform_dim_clientes.py`: Fully implemented with SCD Type 2
- ✅ `transform_dim_usuarios.py`: Fully implemented with SCD Type 2 and hierarchy resolution
- ✅ `transform_fact_faturamento.py`: Fully implemented with FK validation

### Bronze to Silver Mapping

| Bronze Table | Records | Silver Table | Records | Transformation Type |
|--------------|---------|--------------|---------|---------------------|
| `bronze.data` | 4,018 | `silver.dim_tempo` | 4,018 ✅ | Enrichment (business calendar) |
| `bronze.contas_base_oficial` | 6 | `silver.dim_clientes` | 9 ✅ | SCD Type 2, data quality, CNPJ/CPF formatting |
| `bronze.usuarios` | 6 | `silver.dim_usuarios` | 5 ✅ | SCD Type 2, hierarchy resolution (gestor) |
| `bronze.usuarios` | 6 | `silver.dim_canal` | 7 ✅ | Extract & normalize (canal_1, canal_2) |
| `bronze.faturamento` | 9 | `silver.fact_faturamento` | 9 ✅ | FK resolution, measures calculation |

**Transformation Details:**
- **dim_tempo**: Enriches date with fiscal calendar, holidays, work days flags
- **dim_clientes**: Standardizes CNPJ/CPF, applies business rules (porte_empresa, categoria_risco, tempo_cliente_dias)
- **dim_usuarios**: Resolves manager hierarchy (email_lider → sk_gestor FK), splits channel data
- **dim_canal**: Extracts unique channels from usuarios (canal_1, canal_2), adds commission/targets
- **fact_faturamento**: Resolves all dimension FKs, calculates derived measures (valor_liquido = valor_bruto - valor_desconto)

### SCD Type 2 Implementation

Dimensions `dim_clientes` and `dim_usuarios` use SCD Type 2:
- `data_inicio` / `data_fim`: Valid date range
- `flag_ativo`: Current record indicator (boolean)
- `versao`: Version number
- `hash_registro`: MD5 hash for change detection
- `motivo_mudanca`: Reason for new version

## Database Configuration

- **Database Type**: PostgreSQL 15 (Azure-hosted)
- **Connection**: Configured in `docker/docker-compose.yml` and `.env` file
- **Current Host**: `creditsdw.postgres.database.azure.com`
- **Timezone**: America/Sao_Paulo

### Database Roles and Permissions

**Roles:**
- `creditsdw`: ETL service account (CREATEDB, LOGIN) - Full access to all schemas
- `dw_admin`: Admin role group - Full access to Bronze/Credits, manages grants
- `dw_developer`: Developer role group - CRUD on all tables (no TRUNCATE on Bronze)
- `dw_reader`: Read-only role group - SELECT only on all tables

**Grants by Schema:**

| Role | Bronze/Credits | Silver |
|------|---------------|--------|
| `creditsdw` | ALL privileges (including TRUNCATE) | ALL privileges |
| `dw_admin` | ALL privileges | No direct access |
| `dw_developer` | SELECT, INSERT, UPDATE, DELETE | SELECT, INSERT, UPDATE, DELETE |
| `dw_reader` | SELECT | SELECT |

## Python Dependencies

Key packages (from `requirements.txt`):
- `pandas==2.1.4` - Data processing
- `psycopg2-binary==2.9.9` - PostgreSQL driver
- `loguru==0.7.2` - Logging (though code uses standard logging)
- `black`, `ruff`, `mypy` - Code quality tools
- `pytest`, `pytest-cov` - Testing

## Environment File

The `.env` file in project root must contain:
```
DB_HOST=<your_host>
DB_PORT=<your_port>
DB_NAME=<your_database>
DB_USER=<your_user>
DB_PASSWORD=<your_password>
```

This file is git-ignored for security.

## Workflow de Desenvolvimento

### Fazendo Alterações em Ingestores

1. Modificar código do ingestor em `python/ingestors/csv/`
2. Testar localmente: `docker compose exec etl-processor python python/ingestors/csv/ingest_<name>.py`
3. Verificar logs: `tail -f logs/<script_name>.log`
4. Validar dados: Query na tabela Bronze
5. Se bem-sucedido, fazer commit das alterações

### Fazendo Alterações em Transformadores

1. Modificar código do transformador em `python/transformers/silver/`
2. Garantir que dimensões sejam carregadas antes de fatos (ordem de dependência)
3. Testar transformador: `docker compose exec etl-processor python python/transformers/silver/transform_<name>.py`
4. Validar dados na tabela Silver
5. Verificar `credits.silver_control` para rastreamento de execução

### Dicas de Debugging

**Visualizar logs em tempo real:**
```bash
docker compose exec etl-processor tail -f /app/logs/<script_name>.log
```

**Acessar banco de dados diretamente:**
```bash
# Usar psql ou qualquer cliente PostgreSQL com credenciais do .env
psql -h <DB_HOST> -U <DB_USER> -d <DB_NAME>
```

**Inspecionar arquivos processados:**
```bash
ls -lh docker/data/processed/
```

**Verificar status do container:**
```bash
cd docker
docker compose ps
docker compose logs etl-processor
```

## Data Quality System (v2.0)

### Validation Modules

#### `python/utils/validators.py` (360 lines)
Comprehensive validation functions for Bronze layer:
- `validar_campo_obrigatorio()` - Required field validation
- `validar_data()` - Date format and validity
- `validar_email()` - Email format (regex)
- `validar_cnpj_cpf()` - CNPJ/CPF with check digits
- `validar_numero()` - Numeric type validation
- `validar_numero_positivo()` / `validar_numero_nao_negativo()` - Range validation
- `validar_valor_dominio()` - Allowed values validation
- `validar_tamanho_string()` - String length validation
- `validar_campo()` - Composite validator (combines all rules)

#### `python/utils/rejection_logger.py` (260 lines)
Structured rejection logging system:
- `RejectionLogger` class - Buffers rejections for batch insert
- Logs to `credits.logs_rejeicao` table
- Serializes complete records to JSON
- Generates summaries by field and severity
- Helper functions for querying and cleanup

### Rejection Logs Table

All rejected records are stored in `credits.logs_rejeicao`:

```sql
credits.logs_rejeicao (
    id BIGSERIAL PRIMARY KEY,
    execucao_id UUID NOT NULL,          -- FK to credits.historico_atualizacoes
    script_nome VARCHAR(255) NOT NULL,  -- Ingestor script name
    tabela_destino VARCHAR(100),        -- Target Bronze table
    numero_linha INTEGER,               -- CSV line number
    campo_falha VARCHAR(100),           -- Field that failed validation
    motivo_rejeicao TEXT NOT NULL,      -- Clear rejection reason
    valor_recebido TEXT,                -- Value that caused failure
    registro_completo JSONB,            -- Complete record as JSON
    severidade VARCHAR(20),             -- WARNING, ERROR, or CRITICAL
    data_rejeicao TIMESTAMP DEFAULT CURRENT_TIMESTAMP
)
```

**Useful queries:**

```sql
-- View recent rejections by execution
SELECT numero_linha, campo_falha, motivo_rejeicao, valor_recebido
FROM credits.logs_rejeicao
WHERE execucao_id = '<UUID>'
ORDER BY numero_linha;

-- Summary of rejections by field (last 7 days)
SELECT campo_falha, motivo_rejeicao, COUNT(*) as total
FROM credits.logs_rejeicao
WHERE script_nome = 'ingest_faturamento.py'
  AND data_rejeicao >= NOW() - INTERVAL '7 days'
GROUP BY campo_falha, motivo_rejeicao
ORDER BY total DESC;

-- Rejection rate by execution
SELECT
    h.script_nome,
    h.linhas_processadas,
    h.linhas_inseridas,
    COUNT(l.id) as linhas_rejeitadas,
    ROUND(COUNT(l.id)::numeric / NULLIF(h.linhas_processadas, 0) * 100, 2) as taxa_rejeicao_pct
FROM credits.historico_atualizacoes h
LEFT JOIN credits.logs_rejeicao l ON l.execucao_id = h.id
WHERE h.data_inicio >= NOW() - INTERVAL '30 days'
GROUP BY h.id, h.script_nome, h.linhas_processadas, h.linhas_inseridas
ORDER BY h.data_inicio DESC;
```

### Validation Philosophy: Bronze vs Silver

**Bronze Layer (RIGOROUS - v2.0):**
- ✅ **REJECTS invalid data** before insertion
- ✅ Validates all fields against rules
- ✅ Only 100% valid records enter database
- ✅ Invalid records logged to `credits.logs_rejeicao`
- ✅ Strategy: Ensure data quality at entry point

**Silver Layer (Business Rules):**
- Applies business logic and transformations
- Validates referential integrity (FK lookups)
- Blocks execution if critical errors found
- Strategy: Ensure analytical reliability

## Database Improvements Applied

### Fixes Applied (2025-01-10)

**1. Grants para dw_admin na Silver** ✓
- Problema: dw_admin não tinha acesso à Silver
- Fix: `GRANT ALL ON ALL TABLES IN SCHEMA silver TO dw_admin;`
- Status: 35 grants adicionados (7 por tabela x 5 tabelas)

**2. Primary Key em bronze.data** ✓
- Problema: bronze.data sem PK formal
- Fix: `ALTER TABLE bronze.data ADD PRIMARY KEY (data_completa);`
- Impacto: Melhora performance de JOINs, previne duplicatas

### Recommendations for Future

**1. Role Groups vs Users**
- `dw_admin`, `dw_developer`, `dw_reader` são role groups (NOLOGIN)
- Para usar: `GRANT dw_developer TO usuario;`
- Usuários individuais devem receber roles via GRANT

**2. Índices Adicionais (Future)**
```sql
-- Melhorar performance de lookups FK
CREATE INDEX idx_fk_cliente ON silver.fact_faturamento(sk_cliente);
CREATE INDEX idx_fk_data ON silver.fact_faturamento(sk_data);
CREATE INDEX idx_fk_usuario ON silver.fact_faturamento(sk_usuario);
CREATE INDEX idx_fk_canal ON silver.fact_faturamento(sk_canal);

-- Melhorar SCD Type 2 queries
CREATE INDEX idx_clientes_ativo ON silver.dim_clientes(flag_ativo, nk_cnpj_cpf);
CREATE INDEX idx_usuarios_ativo ON silver.dim_usuarios(flag_ativo, nk_usuario);
```

**3. Constraints Validation**
- Todas FKs estão validadas ✓
- Todas PKs estão ativas ✓
- UNIQUE constraints funcionando corretamente ✓

### Performance Considerations

**Bronze Layer:**
- Sem PKs formais (by design) → menos overhead em TRUNCATE/RELOAD
- Surrogate keys (sk_id) disponíveis para queries
- Estratégia TRUNCATE/RELOAD é apropriada para raw data

**Silver Layer:**
- PKs formais em todas tabelas → garantia de qualidade
- FKs com integridade referencial → previne órfãos
- SCD Type 2 com índices em flag_ativo → queries rápidas
- UNIQUE constraints em hash fields → idempotência

**Recommendation: Índices já criados automaticamente:**
- PKs → índice B-tree automático
- FKs → considerar índices manuais se queries lentas (ver acima)
- UNIQUE → índice B-tree automático

## Project Version and Status

**Current Version:** 2.0 (Major refactoring completed November 2025)
**Branch:** `dev` (main branch: `main`)
**Status:** ✅ Production-ready with rigorous validation

### Major Changes in v2.0 (November 2025)

**Breaking Change:** Bronze layer now implements **rigorous validation**
- All ingestors MUST implement `get_validation_rules()` method
- Invalid data is REJECTED before database insertion
- New table `credits.logs_rejeicao` tracks all rejections
- See `RESUMO_REFATORACAO_BRONZE.md` for complete refactoring details

**New Files Added:**
- `python/utils/validators.py` (360 lines) - Comprehensive validation functions
- `python/utils/rejection_logger.py` (260 lines) - Rejection logging system
- `python/ingestors/csv/ingest_data.py` - Date dimension ingestor
- `RESUMO_REFATORACAO_BRONZE.md` - Executive summary of refactoring

**Modified Files:**
- `python/ingestors/csv/base_csv_ingestor.py` - Refactored from 247 to 700 lines
- All ingestors updated with `get_validation_rules()` method
- README.md - Updated with v2.0 architecture

**Removed Files:**
- `python/utils/test_data_cleaner.py` - No longer needed with strict validation

### Implementation Status

**Bronze Layer:** ✅ Fully implemented with rigorous validation
- 4 tables with validation rules
- All ingestors operational
- Rejection logging active

**Silver Layer:** ✅ Fully implemented
- 5 tables (4 dimensions + 1 fact)
- SCD Type 2 working for dim_clientes and dim_usuarios
- All transformers operational

### Recent Commits

1. **feat: implementar validação rigorosa na camada Bronze** (Nov 25, 2025)
   - Complete refactoring with validation system
   - 1,884 lines added, 363 removed
   - New rejection logging system
2. **docs: adicionar resumo executivo da refatoração Bronze**
3. **feat: Silver layer fully functional with sample data**

### Recommended Next Steps

1. **Monitor and tune validation rules:**
   - Analyze rejection patterns in `credits.logs_rejeicao`
   - Adjust domain values if needed
   - Add new validation rules based on business requirements

2. **Expand test coverage:**
   - Unit tests for validation functions
   - Integration tests for Bronze → Silver pipeline
   - Regression tests for SCD Type 2 logic

3. **Performance optimizations:**
   - Add FK indexes on `silver.fact_faturamento` if queries are slow
   - Consider parallelizing validation for large CSVs (>100K rows)
   - Optimize SCD Type 2 queries with partial indexes

4. **Documentation:**
   - Create data dictionary for all fields
   - Document business rules in detail
   - Build analytics query examples

