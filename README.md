# 🏦 Data Warehouse Credits Brasil

> **Versão:** 1.0 | **Arquitetura:** Bronze Layer | **PostgreSQL** 15

---

## 📋 Visão Geral

Solução de Data Warehouse que consolida dados de múltiplas fontes em uma camada Bronze.

### ✨ Recursos Principais

- ✅ **3 tabelas Bronze** - Dados brutos de fontes CSV
- ✅ **Scripts SQL** - Para criação da estrutura inicial do banco de dados
- ✅ **Docker Compose** - Para orquestração de containers

---

## 🏗️ Arquitetura

```
FONTES (CSV) → BRONZE (Raw)
```

- **Bronze:** Dados brutos preservados exatamente como vieram das fontes CSV.

### 📊 Fontes de Dados

| Fonte | Tipo | Frequência | Status |
|-------|------|-----------|--------|
| **Arquivos CSV** | CSV | Manual | ✅ Implementado |

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
psql -U postgres -d credits_dw -f sql/init/01-create-schemas.sql
psql -U postgres -d credits_dw -f sql/bronze/01-create-bronze-tables.sql
```

---

## 💻 Uso

### Colocando Arquivos para Processamento

Copie os arquivos CSV para um diretório de sua escolha.

### Executando Scripts de Ingestão

Os scripts de ingestão de dados ainda estão em desenvolvimento.

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
- ✅ Use roles específicos do PostgreSQL: `dw_developer`

---

## 📞 Suporte

- Para issues: Abra um issue no repositório

---

## 📜 Licença

Propriedade de Credits Brasil © 2025