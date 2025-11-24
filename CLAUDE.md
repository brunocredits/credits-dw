# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Data Warehouse ETL pipeline** for Credits Brasil that ingests CSV files into a PostgreSQL database using a **Bronze Layer architecture**. The project uses Python scripts orchestrated with Docker to process financial data from various sources.

**Key Architecture Concepts:**
- **Bronze Layer**: Raw data storage with minimal transformations (only column renaming and type standardization). Tables have surrogate keys (sk_id) but PKs are not strictly required in raw layer. Uses **TRUNCATE/RELOAD** strategy.
- **Silver Layer**: Dimensional model (Star Schema) with business logic, data quality rules, and SCD Type 2 for historical tracking. Uses **incremental/SCD Type 2** strategy.
- **Schemas**: `bronze` (raw data), `silver` (dimensional model), and `credits` (ETL metadata/audit tables)
- **Date Dimension**: `bronze.data` feeds `silver.dim_tempo` - pre-calculated date dimension with business calendar attributes
- **Audit Trail**: All ETL executions are tracked in `credits.historico_atualizacoes` and Silver loads in `credits.silver_control`

**Current Data Status:**
- Bronze: ✓ Operational (4 tables with test data: 8 contas, 2 usuarios, 2 faturamento)
- Silver: ⚠️ Partially loaded (dim_tempo: 4,018 records, dim_canal: 7 records, others awaiting transformers)

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

### Ingestor Pattern (Template Method)

All CSV ingestors inherit from `BaseCSVIngestor` in `python/ingestors/csv/base_csv_ingestor.py`, which implements the Template Method pattern. When creating a new ingestor:

1. **Inherit from `BaseCSVIngestor`**
2. **Implement two required methods:**
   - `get_column_mapping()`: Returns dict mapping CSV columns to Bronze table columns
   - `get_bronze_columns()`: Returns list of Bronze table column names in order

3. **Call super().__init__()** with:
   - `script_name`: Script filename for audit logs
   - `tabela_destino`: Full table name (e.g., `'bronze.faturamento'`)
   - `arquivo_nome`: CSV filename to process
   - `input_subdir`: Subdirectory under `/app/data/input/` (usually `'onedrive'`)

**Example ingestor structure** (`python/ingestors/csv/ingest_faturamento.py:1-54`):
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
            'Moeda': 'moeda'
        }

    def get_bronze_columns(self) -> List[str]:
        return ['data', 'receita', 'moeda']
```

### ETL Execution Flow (BaseCSVIngestor)

The `executar()` method in `python/ingestors/csv/base_csv_ingestor.py:215-297` orchestrates:

1. File validation
2. Database connection (`utils.db_connection.get_db_connection()`)
3. Audit registration (`utils.audit.registrar_execucao()`)
4. CSV reading (`ler_csv()` - reads all as strings, handles encoding)
5. Bronze transformation (`transformar_para_bronze()` - applies column mapping, formats dates)
6. Database insertion (`inserir_bronze()` - **TRUNCATE/RELOAD strategy**)
7. File archival (moves processed file to `docker/data/processed/` with timestamp)
8. Audit finalization (`utils.audit.finalizar_execucao()`)

**IMPORTANT**: Bronze layer uses **TRUNCATE/RELOAD**, not incremental loads. Each execution completely replaces the table data.

### Database Connection

- Connection logic in `python/utils/db_connection.py`
- Credentials loaded from environment variables (`DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`)
- Two connection modes:
  - `get_db_connection()`: Direct connection (used by ingestors)
  - `get_pooled_connection()`: Connection pooling (available but not currently used)

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

**bronze.contas_base_oficial** (8 records):
- `cnpj_cpf`, `tipo`, `status`, `status_qualificação_da_conta`
- `data_criacao`, `grupo`, `razao_social`, `responsavel_conta` ✓ (typo corrigido de "resposanvel_conta")
- `financeiro`, `corte`, `faixa`, `sk_id` (PK)

**bronze.usuarios** (2 records):
- `nome_empresa`, `Nome`, `area`, `senioridade`, `gestor`, `email`
- `canal_1`, `canal_2` ✓ (renomeado de "canal 1", "canal 2")
- `email_lider`, `sk_id` (PK)

**bronze.faturamento** (2 records):
- `data`, `receita`, `moeda`, `sk_id` (PK)

**bronze.data** (no PK required - reference table):
- `data_completa`, `ano`, `mes`, `dia`, `bimestre`, `trimestre`, `quarter`, `semestre`

## Adding a New CSV Ingestor

1. Create new file in `python/ingestors/csv/ingest_<name>.py`
2. Implement class inheriting from `BaseCSVIngestor`
3. Define column mappings and Bronze column list
4. Add CSV file to `docker/data/input/onedrive/`
5. Create corresponding Bronze table in PostgreSQL
6. Add ingestor instance to `python/run_all_ingestors.py` if it should run with all ingestors

## Silver Layer (Dimensional Model)

The Silver layer implements a **Star Schema** with dimensions and fact tables, applying business rules and data quality transformations.

### Silver Tables

**Dimensions:**
- `silver.dim_tempo` ✓ **(4,018 registros)**
  - PK: `sk_data` (integer)
  - Natural Key: `data_completa` (date, UNIQUE)
  - Atributos: ano, mes, dia, trimestre, semestre, nome_mes, dia_semana, flags (fim_semana, dia_util, feriado)

- `silver.dim_clientes` ⚠️ **(aguardando transformer)**
  - PK: `sk_cliente` (integer)
  - Natural Key: `nk_cnpj_cpf` (varchar, com versioning)
  - SCD Type 2: `data_inicio`, `data_fim`, `flag_ativo`, `versao`, `hash_registro`, `motivo_mudanca`
  - UNIQUE: `uk_cliente_cnpj_versao` (nk_cnpj_cpf + versao)

- `silver.dim_usuarios` ⚠️ **(aguardando transformer)**
  - PK: `sk_usuario` (integer)
  - Natural Key: `nk_usuario` (varchar, hash ou email)
  - FK: `sk_gestor` → `dim_usuarios.sk_usuario` (hierarquia self-referencing)
  - SCD Type 2: `data_inicio`, `data_fim`, `flag_ativo`

- `silver.dim_canal` ✓ **(7 registros)**
  - PK: `sk_canal` (integer)
  - Natural Key: `tipo_canal` + `nome_canal` (combinação única)
  - Atributos: categoria_canal, prioridade, comissao_percentual, meta_mensal

**Facts:**
- `silver.fact_faturamento` ⚠️ **(aguardando transformer)**
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
- ✓ `BaseTransformer`: Fully implemented with SCD Type 2 support
- ✓ `dim_tempo`: Pre-loaded (4,018 records)
- ✓ `dim_canal`: Pre-loaded (7 records)
- ⚠️ `transform_dim_clientes.py`: Template created, awaiting implementation
- ⚠️ `transform_dim_usuarios.py`: Template created, awaiting implementation
- ⚠️ `transform_fact_faturamento.py`: Template created, awaiting implementation

### Bronze to Silver Mapping

| Bronze Table | Records | Silver Table | Records | Transformation Type |
|--------------|---------|--------------|---------|---------------------|
| `bronze.data` | N/A | `silver.dim_tempo` | 4,018 ✓ | Enrichment (business calendar) |
| `bronze.contas_base_oficial` | 8 | `silver.dim_clientes` | 0 ⚠️ | SCD Type 2, data quality, CNPJ/CPF formatting |
| `bronze.usuarios` | 2 | `silver.dim_usuarios` | 0 ⚠️ | SCD Type 2, hierarchy resolution (gestor) |
| `bronze.usuarios` | 2 | `silver.dim_canal` | 7 ✓ | Extract & normalize (canal_1, canal_2) |
| `bronze.faturamento` | 2 | `silver.fact_faturamento` | 0 ⚠️ | FK resolution, measures calculation |

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

## Utilitários de Qualidade de Dados

### Test Data Cleaner

Localização: `python/utils/test_data_cleaner.py`

Utilitário para limpar dados de teste/amostra antes do carregamento:
- Remove caracteres inválidos
- Padroniza formatos de data
- Valida CNPJ/CPF
- Detecta e sinaliza duplicatas

**Uso:**
```bash
docker compose exec etl-processor python python/utils/test_data_cleaner.py
```

### Validação de Qualidade em Bronze vs Silver

**Camada Bronze (Permissiva):**
- Aceita dados problemáticos com WARNINGS
- Registra detalhes nos logs para troubleshooting
- Estratégia: preservar dados de origem

**Camada Silver (Rigorosa):**
- REJEITA dados com problemas de qualidade
- Valida integridade referencial
- Bloqueia execução se houver erros críticos
- Estratégia: garantir qualidade analítica

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

## Contexto de Desenvolvimento Atual

### Estado do Repositório

**Branch atual:** `dev` (branch principal: `main`)

**Arquivos modificados recentemente:**
- `python/ingestors/csv/base_csv_ingestor.py` - Classe base dos ingestores
- `python/run_all_ingestors.py` - Script para executar todos os ingestores
- `python/transformers/base_transformer.py` - Classe base dos transformadores
- `python/transformers/silver/transform_dim_usuarios.py` - Transformador de usuários
- `python/transformers/silver/transform_fact_faturamento.py` - Transformador de faturamento

**Novos arquivos:**
- `pytest.ini` - Configuração de testes
- `python/utils/test_data_cleaner.py` - Utilitário de limpeza de dados
- `tests/` - Diretório de testes unitários

### Commits Recentes

1. **docs: rewrite README for Credits Brasil data team + fix date handling**
2. **fix: corrigir transformadores Silver para execução completa**
3. **merge: sincronizar dev com main para trazer camada Silver**
4. **fix: corrigir mapeamentos de colunas nos ingestores Bronze**
5. **feat: Silver layer fully functional with sample data**

### Próximos Passos Recomendados

1. **Implementar transformadores Silver pendentes:**
   - `transform_dim_clientes.py` - Em template, precisa implementação completa
   - `transform_dim_usuarios.py` - Em desenvolvimento
   - `transform_fact_faturamento.py` - Em desenvolvimento

2. **Expandir cobertura de testes:**
   - Testes unitários para `BaseCSVIngestor`
   - Testes para transformadores Silver
   - Testes de integração Bronze → Silver

3. **Melhorias de performance:**
   - Adicionar índices em FKs da `fact_faturamento`
   - Otimizar queries SCD Type 2
   - Considerar particionamento de tabelas grandes

4. **Documentação:**
   - Adicionar docstrings em métodos complexos
   - Documentar regras de negócio específicas
   - Criar exemplos de queries analíticas

