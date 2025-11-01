# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Data Warehouse solution for Credits Brasil using PostgreSQL 15 with a **Medallion Architecture** (Bronze → Silver → Gold). The system consolidates data from 9+ sources into 16 Bronze tables, 10 Silver tables, and 12+ Gold views for BI analytics.

**Important:** PostgreSQL is managed externally (local installation or remote server). Docker is used ONLY for the Python ETL processing environment.

## Architecture Layers

### Bronze Layer (`bronze` schema)
- **Purpose**: Raw data preservation exactly as received from sources
- **Characteristics**: All fields stored as VARCHAR/TEXT, no validation or transformation
- **Metadata**: Every table includes `data_carga_bronze` and `nome_arquivo_origem`
- **Location**: `sql/bronze/01-create-bronze-tables.sql`

### Silver Layer (`credits` schema - tables)
- **Purpose**: Curated data with proper data types, validation, and relationships
- **Characteristics**:
  - Correct data types (DATE, NUMERIC, BOOLEAN)
  - Primary keys (PKs) and foreign keys (FKs)
  - Deduplication applied
  - Status standardization
  - Multiple source merging where applicable
- **Central Table**: `credits.clientes` merges data from multiple sources
- **Location**: `sql/silver/01-create-silver-tables.sql`

### Gold Layer (`credits` schema - views)
- **Purpose**: Aggregated analytics-ready views for BI dashboards
- **Examples**: `vw_dashboard_executivo`, `vw_faturamento_mensal`, `vw_pipeline_vendas`
- **Location**: `sql/gold/01-create-gold-views.sql`

## Data Sources

| Source | Type | Frequency | Priority |
|--------|------|-----------|----------|
| **Ploomes CRM** | API REST | Daily | ✅ Mandatory (non-negotiable) |
| **OneDrive** | CSV/Excel | Daily | ✅ Implemented |
| **Faturamento (Base)** | CSV | Monthly | 📅 Planned |
| **Consumo (5 sources)** | CSV/JSON/TXT | Monthly | 📅 Planned |

### Data Flow
1. Files are placed in `docker/data/input/[category]/` (shared folder)
2. Python ETL scripts read from input folder
3. Data is loaded into Bronze layer (PostgreSQL)
4. Processed files are moved to `docker/data/processed/` with timestamp
5. Transformations run Bronze → Silver → Gold

## Docker Environment Setup

### Starting the ETL Container
```bash
cd docker
docker-compose up -d --build
```

### Executing ETL Scripts
```bash
# CSV ingestor
docker-compose exec etl-processor python python/ingestors/csv/ingest_onedrive_clientes.py

# API ingestor (Ploomes)
docker-compose exec etl-processor python python/ingestors/api/ingest_ploomes_contacts.py

# Access container shell
docker-compose exec etl-processor bash
```

### Viewing Logs
```bash
# Container logs
docker-compose logs -f etl-processor

# ETL script logs
docker-compose exec etl-processor cat /app/logs/ingest_onedrive_clientes.log
```

## Python ETL Development

### Project Structure
```
python/
├── ingestors/
│   ├── csv/
│   │   ├── base_csv_ingestor.py      # Abstract base class for CSV
│   │   └── ingest_onedrive_clientes.py  # Example CSV ingestor
│   ├── json/
│   │   └── base_json_ingestor.py     # Abstract base class for JSON
│   └── api/
│       ├── ploomes_client.py         # Ploomes API client
│       └── ingest_ploomes_contacts.py  # Ploomes ingestor
├── transformers/                      # Bronze→Silver transformations
└── utils/
    ├── db_connection.py              # PostgreSQL connection management
    ├── logger.py                     # Logging configuration
    └── audit.py                      # ETL execution auditing
```

### Creating a New CSV Ingestor

Use the Template Method pattern by extending `BaseCSVIngestor`:

```python
#!/usr/bin/env python3
import sys
from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class IngestMyData(BaseCSVIngestor):
    def __init__(self):
        super().__init__(
            script_name='ingest_my_data.py',
            tabela_destino='bronze.my_table',
            arquivo_nome='my_data.csv',
            input_subdir='onedrive'  # subfolder in docker/data/input/
        )
        # Override CSV settings if needed
        self.csv_separator = ','  # default is ';'
        self.csv_encoding = 'utf-8'  # default is 'utf-8-sig'

    def get_column_mapping(self):
        """Map CSV columns to Bronze table columns"""
        return {
            'CSV_Column_Name': 'bronze_column_name',
            'Another_CSV_Col': 'another_bronze_col'
        }

    def get_bronze_columns(self):
        """List all Bronze table columns in order"""
        return ['bronze_column_name', 'another_bronze_col']

if __name__ == '__main__':
    ingestor = IngestMyData()
    sys.exit(ingestor.executar())
```

### Creating a New API Ingestor (Ploomes)

The Ploomes client handles authentication, pagination, and retry logic automatically:

```python
#!/usr/bin/env python3
from ingestors.api.ploomes_client import PloomesClient
from utils.db_connection import get_db_connection
from utils.audit import registrar_execucao, finalizar_execucao

# Initialize client (reads PLOOMES_API_KEY from .env)
client = PloomesClient()

# Get all contacts with automatic pagination
contacts = client.get_all_contacts()

# Get all deals
deals = client.get_all_deals()

# Get all companies
companies = client.get_all_companies()

# Always close when done
client.close()
```

### Environment Variables

Required variables in `.env`:

```bash
# Database connection (external PostgreSQL)
DB_HOST=localhost              # or IP of remote server
DB_PORT=5432
DB_NAME=credits_dw
DB_USER=dw_developer
DB_PASSWORD=your_password

# Ploomes API (mandatory)
PLOOMES_API_KEY=your_api_key
PLOOMES_API_URL=https://api2.ploomes.com

# Data paths
DATA_INPUT_PATH=./docker/data/input
DATA_PROCESSED_PATH=./docker/data/processed

# Logging
LOG_LEVEL=INFO
LOG_PATH=./docker/logs
```

### Audit System

All ETL executions are tracked in `credits.historico_atualizacoes`. The base classes handle this automatically, but for custom scripts:

```python
from utils.audit import registrar_execucao, finalizar_execucao

# Start execution
execucao_id = registrar_execucao(
    conn=conn,
    script_nome='my_script.py',
    camada='bronze',
    tabela_destino='bronze.my_table'
)

# ... process data ...

# Finish with success
finalizar_execucao(
    conn=conn,
    execucao_id=execucao_id,
    status='sucesso',
    linhas_processadas=1000,
    linhas_inseridas=1000
)

# Or finish with error
finalizar_execucao(
    conn=conn,
    execucao_id=execucao_id,
    status='erro',
    mensagem_erro=str(exception)
)
```

## Database Management

### Database is External (Not in Docker)
PostgreSQL is managed separately - either running locally or on a remote server. The Docker container connects to it via `DB_HOST` environment variable.

### Database Roles
- `dw_admin`: Superuser with full privileges
- `dw_developer`: Read/write on Bronze and Silver layers
- `dw_analyst`: Read-only on Silver/Gold (credits schema)
- `dw_viewer`: Read-only on Gold views only

### Connecting from Docker to Local PostgreSQL
Use `host.docker.internal` as `DB_HOST` to connect to PostgreSQL running on the host machine.

```bash
# In .env
DB_HOST=host.docker.internal
```

## SQL Scripts Folder

### Purpose of `sql/` Directory

**Keep the SQL scripts** - they serve multiple important purposes:

1. **Documentation**: Complete structure reference
2. **Version Control**: Track schema changes via Git
3. **Disaster Recovery**: Rebuild database from scratch if needed
4. **New Environments**: Create staging/dev environments
5. **Onboarding**: Help new developers understand the complete structure

### When to Use SQL Scripts
- Initial database setup
- Adding new tables/views
- Server migration
- Creating test environments
- Documentation reference

### Initialization Order
```bash
psql -U postgres -d credits_dw -f sql/init/01-create-schemas.sql
psql -U postgres -d credits_dw -f sql/bronze/01-create-bronze-tables.sql
psql -U postgres -d credits_dw -f sql/silver/01-create-silver-tables.sql
psql -U postgres -d credits_dw -f sql/gold/01-create-gold-views.sql
```

## Key Design Principles

1. **Bronze Layer**: Never transform or validate. Store everything as-is (VARCHAR/TEXT) with metadata
2. **Silver Layer**: Apply business rules, standardize categories, enforce referential integrity
3. **Gold Layer**: Aggregated views optimized for BI tools
4. **Audit Trail**: Every ETL operation must register in `credits.historico_atualizacoes`
5. **Data Loading Strategy**: Bronze tables use TRUNCATE/RELOAD; processed files move to backup folder
6. **File Processing**: Files in `docker/data/input/` → processed → moved to `docker/data/processed/` with timestamp
7. **Ploomes API**: Only data source that uses API (non-negotiable requirement)
8. **Monthly Processing**: Faturamento and Consumo are loaded once per month

## Common Development Tasks

### Testing Database Connection
```bash
docker-compose exec etl-processor python -c "from utils.db_connection import get_db_connection; conn = get_db_connection(); print('✓ Connected'); conn.close()"
```

### Testing Ploomes API
```bash
docker-compose exec etl-processor python python/ingestors/api/ploomes_client.py
```

### Checking Audit Log
```sql
SELECT
    script_nome,
    camada,
    status,
    linhas_processadas,
    data_inicio,
    data_fim
FROM credits.historico_atualizacoes
ORDER BY data_inicio DESC
LIMIT 20;
```

### Local Development (Without Docker)
```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate  # Linux/Mac

# Install dependencies
pip install -r requirements.txt

# Run script
python python/ingestors/csv/ingest_onedrive_clientes.py
```

## Code Quality

```bash
# Format code
black python/

# Lint
flake8 python/

# Type check
mypy python/

# Run tests
pytest
```

## Troubleshooting

### Container Can't Connect to PostgreSQL
- Check `DB_HOST` is set to `host.docker.internal` for local database
- Verify PostgreSQL accepts connections from Docker network
- Check firewall rules

### Ploomes API Errors
- Verify `PLOOMES_API_KEY` is correct in `.env`
- Check API rate limits
- Ensure API endpoint URL is correct

### File Not Found Errors
- Ensure files are in correct subfolder of `docker/data/input/`
- Check file permissions
- Verify `input_subdir` parameter matches folder structure
