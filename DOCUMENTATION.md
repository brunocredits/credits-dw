# 🏦 Data Warehouse Credits Brasil

> **Versão:** 1.0 | **Arquitetura:** Bronze Layer | **PostgreSQL** 15

---

## 📋 Visão Geral

Solução de Data Warehouse que consolida dados de múltiplas fontes em uma camada Bronze em um banco de dados PostgreSQL. O objetivo principal é criar uma fonte única de verdade para dados brutos, que podem ser usados para análises e relatórios.

### ✨ Recursos Principais

- ✅ **Tabelas Bronze** - Dados brutos de fontes CSV e API Ploomes
- ✅ **Scripts SQL** - Para criação da estrutura inicial do banco de dados
- ✅ **Docker Compose** - Para orquestração de containers
- ✅ **Scripts de Ingestão Python** - Para ETL de CSV e API

---

## 🏗️ Arquitetura

```
FONTES (CSV, API) → BRONZE (Raw)
```

- **Bronze:** Dados brutos preservados com o mínimo de transformação, garantindo que os dados brutos sejam preservados em seu formato original.

### 📊 Fontes de Dados

| Fonte | Tipo | Frequência | Status |
|-------|------|-----------|--------|
| **Arquivos CSV** | CSV | Manual | ✅ Implementado |
| **Ploomes API** | API | Manual | ✅ Implementado |

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
│       ├── csv/
│       └── api/
├── .gitignore
├── requirements.txt
└── README.md
```

---

## 🚀 Instalação e Setup

### Pré-requisitos

- Docker e Docker Compose
- Python 3.10+
- PostgreSQL 15

### Quick Start

#### 1. Clonar repositório
```bash
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw
```

#### 2. Configurar ambiente
Crie um arquivo `.env` com as seguintes variáveis:
```
DB_HOST=...
DB_PORT=...
DB_NAME=...
DB_USER=...
DB_PASSWORD=...
PLOOMES_API_KEY=...
```

#### 3. Inicializar banco de dados
```bash
psql -U postgres -d credits_dw -f sql/init/01-create-schemas.sql
psql -U postgres -d credits_dw -f sql/bronze/01-create-bronze-tables.sql
```

---

## 💻 Uso

O processo de ETL é executado usando Docker Compose.

### 1. Iniciar o container
```bash
cd docker && docker-compose up -d
```

### 2. Executar um script de ETL
```bash
docker-compose exec etl-processor python python/ingestors/csv/ingest_onedrive_clientes.py
```
ou
```bash
docker-compose exec etl-processor python python/ingestors/api/ingest_ploomes_contacts.py
```

---

## 🛠️ Desenvolvimento

### Code Quality

O projeto usa as seguintes ferramentas para garantir a qualidade do código:

```bash
# Formatação
black python/

# Linting
flake8 python/

# Type checking
mypy python/
```

### Testing

O projeto usa `pytest` para testes. (TODO: Adicionar instruções sobre como executar os testes).

---

## 🔒 Segurança

- ✅ Arquivo `.env` **NUNCA** deve ser commitado (já está no `.gitignore`)
- ✅ Use roles específicos do PostgreSQL: `dw_developer`

---

## 📞 Suporte

- Para issues: Abra um issue no repositório

---

## 📜 Licença

Propriedade de Credits Brasil © 2025
