# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a **Data Warehouse ETL pipeline** for Credits Brasil that ingests CSV files into a PostgreSQL database using a **Bronze Layer architecture**. The project uses Python 3.10+ scripts orchestrated with Docker to process financial data from various sources.

**Key Architecture Concepts:**
- **Bronze Layer**: Raw data storage with minimal transformations (only column renaming and type standardization)
- **Schemas**: `bronze` (raw data tables) and `credits` (ETL metadata/audit tables)
- **Date Dimension**: `bronze.data` is a pre-calculated date dimension table for performance optimization in time-based queries
- **Audit Trail**: All ETL executions are tracked in `credits.historico_atualizacoes`
- **Template Method Pattern**: All ingestors inherit from `BaseCSVIngestor` which provides the execution framework
- **Configuration**: Centralized in `python/utils/config.py` using dataclasses with environment variable loading

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
# Execute ALL CSV ingestors at once (sequentially)
docker compose exec etl-processor python python/run_all_ingestors.py

# Execute in parallel with default workers (3)
docker compose exec etl-processor python python/run_all_ingestors.py --parallel

# Execute in parallel with custom worker count
docker compose exec etl-processor python python/run_all_ingestors.py --parallel --workers 5

# Execute specific ingestors only
docker compose exec etl-processor python python/run_all_ingestors.py --scripts faturamento usuarios

# List available ingestors
docker compose exec etl-processor python python/run_all_ingestors.py --list

# Execute a specific ingestor directly
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py
docker compose exec etl-processor python python/ingestors/csv/ingest_usuarios.py
docker compose exec etl-processor python python/ingestors/csv/ingest_contas_base_oficial.py
```

### Code Quality

```bash
# Format code with Black
black python/

# Lint code with Ruff
ruff check python/

# Type checking with mypy
mypy python/

# Run all quality checks at once
black python/ && ruff check python/ && mypy python/
```

### Local Development (Without Docker)

```bash
# Set up environment
python3.10 -m venv venv
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Set environment variables (ensure .env file exists)
export $(cat .env | xargs)

# Run a specific ingestor
PYTHONPATH=. python3 python/ingestors/csv/ingest_faturamento.py

# Or run all ingestors
PYTHONPATH=. python3 python/run_all_ingestors.py
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
- Uses **context managers** for automatic resource cleanup
- Includes **retry logic** with exponential backoff using tenacity
- Two connection modes:
  - `get_db_connection()`: Context manager for direct connection (primary method)
  - `get_pooled_connection()`: Connection pooling (available but not currently used)
  - `get_cursor()`: Context manager for cursor handling

**Example usage:**
```python
from utils.db_connection import get_db_connection, get_cursor

# Connection context manager
with get_db_connection() as conn:
    with get_cursor(conn) as cursor:
        cursor.execute("SELECT * FROM bronze.faturamento")
        results = cursor.fetchall()
```

### Audit System

All ETL runs are tracked in `credits.historico_atualizacoes`:
- `registrar_execucao()`: Logs start with status `'em_execucao'`, returns execution ID
- `finalizar_execucao()`: Updates with final status (`'sucesso'` or `'erro'`), line counts, and error messages
- Tracks: script name, start/end times, lines processed/inserted, errors, and execution duration

**Key audit fields:**
- `script_nome`: Name of the ingestor script
- `camada`: Data warehouse layer ('bronze', 'silver', 'gold')
- `tabela_origem`: Source table (usually NULL for CSV ingestion)
- `tabela_destino`: Destination table (e.g., 'bronze.faturamento')
- `data_inicio` / `data_fim`: Start and end timestamps
- `status`: 'em_execucao', 'sucesso', or 'erro'
- `linhas_processadas` / `linhas_inseridas`: Row counts
- `mensagem_erro`: Error message if status is 'erro'

### File Paths (Inside Docker Container)

- **Input CSVs**: `/app/data/input/onedrive/`
- **Processed Archive**: `/app/data/processed/`
- **Templates**: `/app/data/templates/` (example CSV files with headers)
- **Logs**: `/app/logs/`

These are mounted from the host `docker/data/` directory.

### Date Handling

Columns starting with `data_` or `dt_` are automatically converted to `YYYY-MM-DD` format by `_formatar_colunas_data()` in `BaseCSVIngestor`. The method:
- Detects date columns by name prefix (`data_` or `dt_`)
- Attempts parsing with multiple date formats
- Converts valid dates to `YYYY-MM-DD` string format
- Replaces invalid dates with `None`
- Logs warnings for unparseable date values

This automatic formatting can be disabled by setting `format_dates=False` in the ingestor constructor.

## Adding a New CSV Ingestor

Follow these steps to add a new CSV ingestor to the pipeline:

1. **Create the ingestor file** in `python/ingestors/csv/ingest_<name>.py`:
```python
from ingestors.csv.base_csv_ingestor import BaseCSVIngestor
from typing import Dict, List

class IngestMyData(BaseCSVIngestor):
    def __init__(self):
        super().__init__(
            script_name='ingest_my_data.py',
            tabela_destino='bronze.my_data',
            arquivo_nome='my_data.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self) -> Dict[str, str]:
        """Map CSV column names to Bronze table columns"""
        return {
            'CSV Column Name': 'bronze_column_name',
            'Another Column': 'another_column'
        }

    def get_bronze_columns(self) -> List[str]:
        """Return Bronze table columns in order"""
        return ['bronze_column_name', 'another_column']

if __name__ == '__main__':
    import sys
    sys.exit(IngestMyData().executar())
```

2. **Create the Bronze table** in PostgreSQL:
```sql
CREATE TABLE bronze.my_data (
    bronze_column_name TEXT,
    another_column TEXT,
    data_carga_bronze TIMESTAMP DEFAULT NOW()
);
```

3. **Register in orchestrator** (`python/run_all_ingestors.py`):
```python
from ingestors.csv.ingest_my_data import IngestMyData

INGESTORS_REGISTRY = {
    'contas': IngestContasBaseOficial,
    'faturamento': IngestFaturamento,
    'usuarios': IngestUsuarios,
    'my_data': IngestMyData,  # Add here
}
```

4. **Add CSV file** to `docker/data/input/onedrive/my_data.csv`

5. **Test the ingestor**:
```bash
# Test individual ingestor
docker compose exec etl-processor python python/ingestors/csv/ingest_my_data.py

# Test with orchestrator
docker compose exec etl-processor python python/run_all_ingestors.py --scripts my_data
```

## Database Configuration

- **Database Type**: PostgreSQL 15 (Azure-hosted)
- **Connection**: Configured in `docker/docker-compose.yml` and `.env` file
- **Current Host**: `creditsdw.postgres.database.azure.com`
- **Timezone**: America/Sao_Paulo

## Python Dependencies

Key packages (from `requirements.txt`):
- `pandas==2.1.4` - Data processing and CSV handling
- `psycopg2-binary==2.9.9` - PostgreSQL driver
- `loguru==0.7.2` - Advanced logging with rotation and colors
- `tenacity` - Retry logic with exponential backoff
- `black`, `ruff`, `mypy` - Code quality and formatting tools
- `pytest`, `pytest-cov` - Testing framework (tests not yet implemented)

## Environment File

The `.env` file in project root must contain these required variables:
```bash
# Required Database Configuration
DB_HOST=your_host.postgres.database.azure.com
DB_PORT=5432
DB_NAME=creditsdw
DB_USER=your_user
DB_PASSWORD=your_password

# Optional Configuration (with defaults)
LOG_LEVEL=INFO                    # DEBUG, INFO, WARNING, ERROR
ETL_MAX_RETRIES=3                 # Number of retry attempts
ETL_RETRY_DELAY=5                 # Seconds between retries
ETL_BATCH_SIZE=1000               # Rows per batch insert
ETL_PARALLEL_INGESTORS=1          # Workers for parallel execution
CSV_SEPARATOR=;                   # CSV delimiter
CSV_ENCODING=utf-8-sig            # CSV encoding (handles BOM)
CSV_CHUNK_SIZE=10000              # Rows per chunk when reading
ENVIRONMENT=development           # development/staging/production
```

This file is git-ignored for security. Use `.env.example` as a template.

## Logging and Monitoring

The project uses **Loguru** for advanced logging capabilities:

- **Log Location**: `/app/logs/` (inside container) or `docker/logs/` (host)
- **Log Files**: Named `{script_name}.log`
- **Rotation**: Automatic at 100 MB
- **Retention**: 30 days
- **Compression**: Old logs compressed to `.zip`
- **Format**: Timestamp, level, module, function, line, and message

**Key logging functions** in `python/utils/logger.py`:
- `setup_logger(name)`: Initialize logger for a script
- `log_dataframe_info(df, nome)`: Log DataFrame statistics (shape, columns, memory, nulls)
- `log_execution_summary(...)`: Log execution results with metrics

**Log levels** (set via `LOG_LEVEL` env var):
- `DEBUG`: Detailed diagnostic information
- `INFO`: General informational messages (default)
- `WARNING`: Warning messages for potential issues
- `ERROR`: Error messages for failures

## Configuration System

The project uses centralized configuration in `python/utils/config.py`:

**Key configuration classes:**
- `DatabaseConfig`: Database connection settings
- `CSVConfig`: CSV parsing defaults
- `PathsConfig`: File system paths
- `ETLConfig`: ETL process settings
- `Config`: Main configuration container (singleton)

**Usage in code:**
```python
from utils.config import get_config, get_db_config, get_etl_config

# Get full config
config = get_config()
print(config.database.host)
print(config.etl.batch_insert_size)

# Get specific config sections
db_config = get_db_config()
etl_config = get_etl_config()
```

The configuration automatically:
- Validates required environment variables
- Provides sensible defaults
- Detects Docker vs local environment
- Uses singleton pattern for consistency

## Important Implementation Notes

1. **TRUNCATE/RELOAD Strategy**: Bronze tables are completely replaced on each run, not incrementally loaded. This means `inserir_bronze()` method in `BaseCSVIngestor` uses `TRUNCATE TABLE` before inserting new data.

2. **CSV Encoding**: The default encoding is `utf-8-sig` which handles UTF-8 files with BOM (Byte Order Mark) commonly created by Excel.

3. **All Strings**: CSV data is initially read with `dtype=str` to preserve original formats. Type conversions happen during transformation.

4. **Batch Inserts**: For performance, inserts use `executemany()` with configurable batch sizes (default 1000 rows).

5. **File Archival**: Successfully processed CSV files are moved to `/app/data/processed/` with a timestamp suffix to prevent reprocessing.

6. **Error Handling**: All errors are logged to both console and file, and recorded in the audit table with full error messages.

7. **Resource Management**: All database connections and cursors use context managers to ensure proper cleanup even if errors occur.

8. **Parallel Execution**: When using `--parallel` mode, ingestors run in separate threads (not processes), sharing the same database connection pool.
