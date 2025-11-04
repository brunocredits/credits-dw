# 🏦 Data Warehouse Credits Brasil

> **Versão:** 2.0 | **Arquitetura:** Bronze Layer | **PostgreSQL** 15

---

## 📋 Visão Geral

Solução de Data Warehouse que consolida dados de múltiplas fontes em uma camada Bronze em um banco de dados PostgreSQL. O objetivo principal é criar uma fonte única de verdade para dados brutos, que podem ser usados para análises e relatórios.

### ✨ Recursos Principais

- ✅ **4 tabelas Bronze** - Dados brutos de fontes CSV
- ✅ **Scripts SQL** - Para criação da estrutura inicial do banco de dados
- ✅ **Docker Compose** - Para orquestração de containers
- ✅ **Scripts de Ingestão Python** - Para ETL de CSV

---

## 🏗️ Arquitetura

```
FONTES (CSV) → BRONZE (Raw)
```

- **Bronze:** Dados brutos preservados com o mínimo de transformação, garantindo que os dados brutos sejam preservados em seu formato original.

### 📊 Fontes de Dados

| Fonte | Tipo | Frequência | Status |
|-------|------|-----------|--------|
| **contas_base_oficial.csv** | CSV | Manual | ✅ Implementado |
| **faturamento.csv** | CSV | Manual | ✅ Implementado |
| **data.csv** | CSV | Manual | ✅ Implementado |
| **usuarios.csv** | CSV | Manual | ✅ Implementado |

---

## 📂 Estrutura do Projeto

```
credits-database/
├── docker/
│   ├── Dockerfile
│   └── docker-compose.yml
├── sql/
│   ├── init/                   # Schemas e roles
│   └── bronze/                 # Tabelas DDL
├── python/
│   └── ingestors/
│       └── csv/
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🚀 Instalação e Setup

### Pré-requisitos

- Docker 20+ e Docker Compose
- Python 3.10+ (para desenvolvimento local)
- PostgreSQL 15 (gerenciado externamente)

### Quick Start

#### 1. Clonar repositório
```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

#### 2. Configurar ambiente
Crie um arquivo `.env` com as credenciais do banco de dados.

**Variáveis OBRIGATÓRIAS:**
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`

#### 3. Inicializar banco de dados
```bash
psql -U <user> -d <database> -f sql/init/01-create-schemas.sql
psql -U <user> -d <database> -f sql/bronze/01-create-bronze-tables.sql
```

---

## 💻 Uso

### Colocando Arquivos para Processamento

Copie os arquivos CSV para o diretório `docker/data/input/onedrive`.

### Executando Scripts de Ingestão

Para executar um script de ingestão, use o `docker-compose exec`. Por exemplo, para ingerir o arquivo `contas_base_oficial.csv`:

```bash
docker compose exec etl-processor python python/ingestors/csv/ingest_contas_base_oficial.py
```

---

## 🛠️ Desenvolvimento

### Code Quality

```bash
# Formatação
black python/

# Linting
flake8 python/

# Type checking
mypy python/
```

---

## 🔒 Segurança

- ✅ Arquivo `.env` **NUNCA** deve ser commitado (já está no `.gitignore`)
- ✅ Use roles específicos do PostgreSQL.

---

## 📞 Suporte

- Para issues: Abra um issue no repositório

---

## 📜 Licença

Propriedade de Credits Brasil © 2025
