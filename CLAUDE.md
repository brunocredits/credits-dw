# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Data Warehouse ETL pipeline** for Credits Brasil that ingests CSV files into a PostgreSQL database using a **Bronze Layer architecture**. The project uses Python scripts orchestrated with Docker to process financial data from various sources.

**Key Architecture Concepts:**
- **Bronze Layer**: Raw data storage with minimal transformations (only column renaming and type standardization)
- **Schemas**: `bronze` (raw data tables) and `credits` (ETL metadata/audit tables)
- **Date Dimension**: `bronze.data` is a pre-calculated date dimension table for performance optimization in time-based queries
- **Audit Trail**: All ETL executions are tracked in `credits.historico_atualizacoes`

## Common Commands

All commands should be run from the project root directory.

### Docker Operations

```bash
# Build and start the ETL container
cd docker && docker compose up -d --build

# Stop the environment
docker compose down

# Access container shell for debugging
docker compose exec etl-processor bash
```

### Running ETL Scripts

```bash
# Execute ALL CSV ingestors at once
docker compose exec etl-processor python python/run_all_ingestors.py

# Execute a specific ingestor
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py
docker compose exec etl-processor python python/ingestors/csv/ingest_usuarios.py
docker compose exec etl-processor python python/ingestors/csv/ingest_contas_base_oficial.py
```

### Code Quality

```bash
# Format code
black python/

# Lint code
ruff check .

# Type checking
mypy python/
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

## Adding a New CSV Ingestor

1. Create new file in `python/ingestors/csv/ingest_<name>.py`
2. Implement class inheriting from `BaseCSVIngestor`
3. Define column mappings and Bronze column list
4. Add CSV file to `docker/data/input/onedrive/`
5. Create corresponding Bronze table in PostgreSQL
6. Add ingestor instance to `python/run_all_ingestors.py` if it should run with all ingestors

## Database Configuration

- **Database Type**: PostgreSQL 15 (Azure-hosted)
- **Connection**: Configured in `docker/docker-compose.yml` and `.env` file
- **Current Host**: `creditsdw.postgres.database.azure.com`
- **Timezone**: America/Sao_Paulo

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
