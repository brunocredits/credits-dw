# 🏦 Data Warehouse Credits Brasil

> **Versão:** 2.0 | **Arquitetura:** Medallion (Bronze → Silver → Gold) | **PostgreSQL** 15

---

## 📋 Visão Geral

Solução completa de Data Warehouse que consolida dados de múltiplas fontes usando arquitetura Medallion de três camadas.

### ✨ Recursos Principais

- ✅ **16 tabelas Bronze** - Dados brutos de todas as fontes
- ✅ **10 tabelas Silver** - Dados limpos, validados e relacionados
- ✅ **12+ views Gold** - Métricas prontas para BI
- ✅ **Scripts ETL Python** - Automação completa com classes base reutilizáveis
- ✅ **Docker Compose** - Container Python para processamento ETL
- ✅ **Auditoria completa** - Rastreabilidade de todas as operações
- ✅ **API Ploomes** - Cliente Python com paginação automática

---

## 🏗️ Arquitetura

```
FONTES → BRONZE (Raw) → SILVER (Curated) → GOLD (Analytics) → Power BI
```

- **Bronze:** Dados brutos preservados exatamente como vieram (tudo VARCHAR/TEXT)
- **Silver:** Dados transformados, validados, com PKs e FKs (tipos corretos)
- **Gold:** Views agregadas para análises e dashboards

### 📊 Fontes de Dados

| Fonte | Tipo | Frequência | Status |
|-------|------|-----------|--------|
| **Ploomes CRM** | API REST | Diária | ✅ Obrigatório |
| **OneDrive** | CSV/Excel | Diária | ✅ Implementado |
| **Faturamento** | CSV | Mensal | 📅 Planejado |
| **Consumo (5 fontes)** | CSV/JSON/TXT | Mensal | 📅 Planejado |

---

## 📂 Estrutura do Projeto

```
credits-database/
├── docker/
│   ├── Dockerfile              # Container Python para ETL
│   ├── docker-compose.yml      # Orquestração do container
│   ├── data/
│   │   ├── input/              # Arquivos para processamento
│   │   │   ├── onedrive/
│   │   │   ├── faturamento/
│   │   │   └── consumo/        # 5 fontes diferentes
│   │   └── processed/          # Arquivos já carregados (backup)
│   └── logs/                   # Logs de execução
├── sql/
│   ├── init/                   # Schemas, roles, funções de auditoria
│   ├── bronze/                 # 16 tabelas DDL
│   ├── silver/                 # 10 tabelas DDL
│   └── gold/                   # 12+ views
├── python/
│   ├── ingestors/
│   │   ├── csv/                # Ingestores CSV (classe base + exemplos)
│   │   ├── json/               # Ingestores JSON/NDJSON
│   │   └── api/                # Cliente Ploomes + ingestores API
│   ├── transformers/           # Transformações Bronze→Silver
│   └── utils/
│       ├── db_connection.py    # Gestão de conexões PostgreSQL
│       ├── logger.py           # Configuração de logs
│       └── audit.py            # Funções de auditoria
├── .env.example                # Template de variáveis de ambiente
├── requirements.txt            # Dependências Python
├── README.md                   # Este arquivo
└── CLAUDE.md                   # Guia para Claude Code
```

---

## 🚀 Instalação e Setup

### Pré-requisitos

- Docker 20+ e Docker Compose
- Python 3.10+ (para desenvolvimento local)
- PostgreSQL 15 (gerenciado externamente - local ou servidor)
- 4GB RAM mínimo

### Quick Start

#### 1. Clonar repositório
```bash
git clone https://github.com/seu-usuario/credits-database.git
cd credits-database
```

#### 2. Configurar ambiente
```bash
# Copiar arquivo de exemplo
cp .env.example .env

# Editar .env com suas credenciais
nano .env  # ou vim, code, etc.
```

**Variáveis OBRIGATÓRIAS:**
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `PLOOMES_API_KEY` (obter em https://ploomes.com)

#### 3. Criar estrutura de diretórios
```bash
mkdir -p docker/data/{input,processed} docker/logs
mkdir -p docker/data/input/{onedrive,faturamento,consumo}
```

#### 4. Inicializar banco de dados (se necessário)
```bash
# Se PostgreSQL estiver sendo configurado pela primeira vez
psql -U postgres -d credits_dw -f sql/init/01-create-schemas.sql
psql -U postgres -d credits_dw -f sql/bronze/01-create-bronze-tables.sql
psql -U postgres -d credits_dw -f sql/silver/01-create-silver-tables.sql
psql -U postgres -d credits_dw -f sql/gold/01-create-gold-views.sql
```

#### 5. Subir container Python ETL
```bash
cd docker
docker-compose up -d --build
```

#### 6. Verificar se container está rodando
```bash
docker-compose ps
docker-compose logs etl-processor
```

---

## 💻 Uso

### Colocando Arquivos para Processamento

```bash
# Copiar arquivos CSV/JSON para pasta compartilhada
cp /origem/Clientes.csv docker/data/input/onedrive/
cp /origem/faturamento_2025-11.csv docker/data/input/faturamento/
```

### Executando Scripts ETL

#### Via Docker (Recomendado)
```bash
cd docker

# Ingerir dados do OneDrive (CSV)
docker-compose exec etl-processor python python/ingestors/csv/ingest_onedrive_clientes.py

# Ingerir dados do Ploomes (API)
docker-compose exec etl-processor python python/ingestors/api/ingest_ploomes_contacts.py

# Acessar shell do container
docker-compose exec etl-processor bash
```

#### Desenvolvimento Local
```bash
# Criar ambiente virtual
python -m venv venv
source venv/bin/activate  # Linux/Mac
# venv\Scripts\activate    # Windows

# Instalar dependências
pip install -r requirements.txt

# Executar script
python python/ingestors/csv/ingest_onedrive_clientes.py
```

### Visualizando Logs

```bash
# Logs do container
docker-compose logs -f etl-processor

# Logs dos scripts ETL
cat docker/logs/ingest_onedrive_clientes.log
```

### Consultando Dados

```sql
-- Verificar execuções ETL
SELECT * FROM credits.historico_atualizacoes
ORDER BY data_inicio DESC
LIMIT 10;

-- Consultar views Gold
SELECT * FROM credits.vw_dashboard_executivo;
SELECT * FROM credits.vw_faturamento_mensal WHERE mes >= '2025-01-01';
```

---

## 📊 Views Gold Disponíveis

| View | Descrição |
|------|-----------|
| `vw_faturamento_mensal` | Receita por mês/cliente/segmento |
| `vw_consumo_mensal_parceiros` | Consumo de serviços por parceiro |
| `vw_pipeline_vendas` | Funil de vendas do Ploomes |
| `vw_performance_atendimento` | Métricas de tickets e SLA |
| `vw_dashboard_executivo` | KPIs principais consolidados |
| ... | +7 views adicionais |

---

## 🛠️ Desenvolvimento

### Criando Novo Ingestor CSV

```python
#!/usr/bin/env python3
from ingestors.csv.base_csv_ingestor import BaseCSVIngestor

class MeuIngestor(BaseCSVIngestor):
    def __init__(self):
        super().__init__(
            script_name='meu_ingestor.py',
            tabela_destino='bronze.minha_tabela',
            arquivo_nome='dados.csv',
            input_subdir='onedrive'
        )

    def get_column_mapping(self):
        return {
            'Coluna_CSV': 'coluna_bronze',
            'Outra_Coluna': 'outra_coluna_bronze'
        }

    def get_bronze_columns(self):
        return ['coluna_bronze', 'outra_coluna_bronze']

if __name__ == '__main__':
    ingestor = MeuIngestor()
    sys.exit(ingestor.executar())
```

### Testando Cliente Ploomes

```bash
# Dentro do container
docker-compose exec etl-processor python python/ingestors/api/ploomes_client.py

# Ou local
python python/ingestors/api/ploomes_client.py
```

### Code Quality

```bash
# Formatação
black python/

# Linting
flake8 python/

# Type checking
mypy python/

# Testes
pytest
```

---

## 🔒 Segurança

- ✅ Arquivo `.env` **NUNCA** deve ser commitado (já está no `.gitignore`)
- ✅ Use roles específicos do PostgreSQL: `dw_developer`, `dw_analyst`, `dw_viewer`
- ✅ Container ETL roda com usuário não-root
- ✅ Senhas padrão devem ser alteradas em produção

---

## 📝 Pasta SQL - Necessária?

### ✅ **SIM, mantenha a pasta `sql/`**

**Motivos:**

1. **Documentação da Estrutura**: Os scripts SQL documentam EXATAMENTE como as tabelas e views foram criadas
2. **Versionamento**: Permite rastrear mudanças na estrutura do banco via Git
3. **Disaster Recovery**: Facilita recriar o DW do zero se necessário
4. **Onboarding**: Novos desenvolvedores entendem a estrutura completa
5. **Ambientes de Teste**: Permite criar ambientes de staging/dev idênticos

**Quando usar:**
- Criação inicial do banco
- Adicionar novas tabelas/views
- Migração para novo servidor
- Documentação de referência

---

## 📞 Suporte

- Para dúvidas sobre o código: Consulte `CLAUDE.md`
- Para issues: Abra um issue no repositório
- Para desenvolvimento com Claude Code: Use `/init` no início da sessão

---

## 📜 Licença

Propriedade de Credits Brasil © 2025
