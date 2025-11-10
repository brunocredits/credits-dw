# 🏦 Credits Brasil - Data Warehouse

> **Pipeline ETL Bronze → Silver** | **PostgreSQL 15** | **Python 3.10+** | **Star Schema**

## 📋 O Que É Este Projeto

Data Warehouse para consolidação de dados financeiros da Credits Brasil, implementando arquitetura **Medallion** (Bronze/Silver) com modelo dimensional Star Schema.

**Status Atual:**
- ✅ Bronze Layer: Ingestão de CSVs operacional
- ✅ Silver Layer: Star Schema implementado (dim_tempo, dim_canal populadas)
- ⚠️ Transformers: Framework pronto, aguardando execução completa

---

## 🏗️ Arquitetura

### Camadas de Dados

```
CSVs → Bronze Layer → Silver Layer → Analytics
       (Raw Data)    (Star Schema)   (BI/Reports)
```

#### **Bronze Layer** - Dados Brutos
Armazena dados originais com mínima transformação (TRUNCATE/RELOAD):

| Tabela | Registros | Descrição |
|--------|-----------|-----------|
| `bronze.contas_base_oficial` | 8 | Clientes B2B |
| `bronze.usuarios` | 2 | Usuários e hierarquia |
| `bronze.faturamento` | 2 | Receitas diárias |
| `bronze.data` | N/A | Dimensão de data (reference) |

#### **Silver Layer** - Modelo Dimensional (Star Schema)
Dados enriquecidos com business rules e SCD Type 2:

| Tabela | Tipo | Registros | Status |
|--------|------|-----------|--------|
| `silver.dim_tempo` | Dimension | 4,018 | ✅ Populada |
| `silver.dim_clientes` | Dimension (SCD2) | 0 | ⚠️ Transformer pronto |
| `silver.dim_usuarios` | Dimension (SCD2) | 0 | ⚠️ Transformer pronto |
| `silver.dim_canal` | Dimension | 7 | ✅ Populada |
| `silver.fact_faturamento` | Fact | 0 | ⚠️ Transformer pronto |

**Relacionamentos:**
```
fact_faturamento (centro) possui 4 Foreign Keys:
├─► dim_clientes (sk_cliente)
├─► dim_usuarios (sk_usuario) - com hierarquia (sk_gestor → sk_usuario)
├─► dim_tempo (sk_data)
└─► dim_canal (sk_canal)
```

#### **Credits Schema** - Auditoria & Controle
| Tabela | Descrição |
|--------|-----------|
| `credits.historico_atualizacoes` | Log de execuções ETL |
| `credits.silver_control` | Controle de transformações Silver |

---

## 🎯 Primary Keys e Foreign Keys

### Por Que Usar PKs e FKs?

**Primary Keys (PKs):**
- ✅ Garantem unicidade de registros
- ✅ Melhoram performance de JOINs (índices automáticos)
- ✅ Previnem duplicatas acidentais
- ✅ Facilitam relacionamentos entre tabelas

**Foreign Keys (FKs):**
- ✅ Garantem integridade referencial (não permite FKs órfãs)
- ✅ Documentam relacionamentos no schema
- ✅ Protegem contra deleções acidentais (CASCADE/RESTRICT)
- ✅ Facilitam análise de relacionamentos

### PKs e FKs no Projeto

**Bronze Layer:**
- Tem surrogate keys (`sk_id`) mas **sem PKs formais** → dados brutos, menos restrições

**Silver Layer:**
- **Todas tabelas têm PKs formais** → garantia de qualidade
- **5 FKs na fact_faturamento** → integridade referencial
- **1 FK self-referencing em dim_usuarios** → hierarquia de gestores

**Exemplo de Integridade:**
```sql
-- Isso FALHARIA se sk_cliente=999 não existir:
INSERT INTO silver.fact_faturamento (sk_cliente, ...) VALUES (999, ...);
-- Error: violates foreign key constraint "fk_fact_faturamento_cliente"
```

---

## 🚀 Como Usar

### 1. Pré-requisitos
- Docker & Docker Compose
- PostgreSQL 15+ (Azure)
- Acesso ao banco configurado

### 2. Configuração Inicial

```bash
# Clonar repositório
git clone https://github.com/brunocredits/credits-dw.git
cd credits-dw

# Configurar credenciais (copie .env.example para .env e edite)
cp .env.example .env
```

### 3. Executar ETL

#### Bronze Layer (Ingestão de CSVs)
```bash
cd docker
docker compose up -d --build

# Executar TODOS os ingestores
docker compose exec etl-processor python python/run_all_ingestors.py

# Executar ingestor específico
docker compose exec etl-processor python python/ingestors/csv/ingest_faturamento.py
```

#### Silver Layer (Transformações)
```bash
# Executar TODAS transformações (dim_clientes, dim_usuarios, fact_faturamento)
docker compose exec etl-processor python python/run_silver_transformations.py

# Executar transformer específico
docker compose exec etl-processor python python/transformers/silver/transform_dim_clientes.py
```

---

## 📂 Estrutura Simplificada

```
credits-dw/
├── docker/
│   ├── Dockerfile
│   ├── docker-compose.yml
│   └── data/
│       ├── input/onedrive/          # CSVs para processar
│       └── processed/               # CSVs já processados
│
├── python/
│   ├── ingestors/csv/               # Bronze: ingestão de CSVs
│   │   ├── base_csv_ingestor.py     # Classe base (Template Method)
│   │   ├── ingest_faturamento.py
│   │   ├── ingest_usuarios.py
│   │   └── ingest_contas_base_oficial.py
│   │
│   ├── transformers/                # Silver: transformações
│   │   ├── base_transformer.py      # Classe base (SCD Type 2)
│   │   └── silver/
│   │       ├── transform_dim_clientes.py
│   │       ├── transform_dim_usuarios.py
│   │       └── transform_fact_faturamento.py
│   │
│   ├── utils/
│   │   ├── config.py                # Configuração centralizada
│   │   ├── db_connection.py         # Context managers + retry
│   │   ├── audit.py                 # Sistema de auditoria
│   │   └── logger.py                # Logging com Loguru
│   │
│   ├── run_all_ingestors.py         # Orquestrador Bronze
│   └── run_silver_transformations.py # Orquestrador Silver
│
├── .env.example                      # Template de configuração
├── requirements.txt                  # Dependências Python
├── README.md                         # Esta documentação
└── CLAUDE.md                         # Guia técnico detalhado
```

---

## 🔑 Conceitos Importantes

### 1. Slowly Changing Dimension (SCD) Type 2
Rastreia histórico de mudanças em dimensões:

```sql
-- Exemplo: Cliente muda de status
-- Registro antigo é "fechado"
UPDATE silver.dim_clientes
SET data_fim = '2025-01-09', flag_ativo = false
WHERE nk_cnpj_cpf = '12345678000199' AND flag_ativo = true;

-- Novo registro é criado
INSERT INTO silver.dim_clientes
(nk_cnpj_cpf, status, data_inicio, flag_ativo, versao)
VALUES ('12345678000199', 'ATIVO', '2025-01-10', true, 2);
```

**Benefícios:**
- Mantém histórico completo de mudanças
- Permite análises temporais ("clientes que eram VIP em 2024")
- Auditoria transparente de alterações

### 2. Star Schema
Modelo dimensional otimizado para análise:

**Fact Table (centro):** Métricas numéricas (valor_bruto, valor_liquido)
**Dimension Tables (pontas):** Contexto (quem, quando, onde, como)

**Vantagens:**
- Queries mais rápidas (menos JOINs)
- SQL mais simples e intuitivo
- Performance previsível
- Fácil de entender para analistas

### 3. Surrogate Keys
Chaves artificiais (sk_cliente, sk_data) ao invés de chaves naturais (CNPJ, data):

**Vantagens:**
- Independentes de mudanças nos dados de negócio
- JOINs mais rápidos (INTEGER vs VARCHAR)
- Suportam SCD Type 2 (múltiplas versões do mesmo cliente)

---

## 📊 Exemplos de Uso (SQL)

### Consulta Analítica Simples
```sql
-- Receita por trimestre em 2024
SELECT
    t.ano,
    t.trimestre,
    SUM(f.valor_liquido) as receita_total,
    COUNT(DISTINCT f.sk_cliente) as clientes_unicos
FROM silver.fact_faturamento f
JOIN silver.dim_tempo t ON f.sk_data = t.sk_data
WHERE t.ano = 2024
GROUP BY t.ano, t.trimestre
ORDER BY t.trimestre;
```

### Análise com SCD Type 2
```sql
-- Clientes que mudaram de status em 2025
SELECT
    nk_cnpj_cpf,
    razao_social,
    versao,
    status,
    data_inicio,
    data_fim
FROM silver.dim_clientes
WHERE data_inicio >= '2025-01-01'
ORDER BY nk_cnpj_cpf, versao;
```

---

## 🛡️ Roles e Permissões

| Role | Bronze/Credits | Silver | Descrição |
|------|---------------|--------|-----------|
| `creditsdw` | ALL (+ TRUNCATE) | ALL | Conta ETL principal |
| `dw_admin` | ALL | - | Administração Bronze |
| `dw_developer` | SELECT, INSERT, UPDATE, DELETE | SELECT, INSERT, UPDATE, DELETE | Desenvolvimento |
| `dw_reader` | SELECT | SELECT | Leitura apenas (BI/Analytics) |

**Segurança:**
- Credenciais em variáveis de ambiente (`.env`)
- Sem senhas hardcoded no código
- Princípio de menor privilégio (readers sem write)

---

## 🐛 Troubleshooting Rápido

**Container não inicia:**
```bash
docker compose logs etl-processor
docker compose down && docker compose up -d --build
```

**Erro de conexão ao banco:**
```bash
# Verificar variáveis
docker compose exec etl-processor env | grep DB_

# Testar conexão
docker compose exec etl-processor python -c "from utils.db_connection import get_db_connection; get_db_connection()"
```

**Transformer falha:**
```bash
# Ver logs detalhados
docker compose exec etl-processor tail -f /app/logs/*.log

# Executar em modo debug
docker compose exec etl-processor python -u python/transformers/silver/transform_dim_clientes.py
```

---

## 📚 Documentação Completa

- **[CLAUDE.md](./CLAUDE.md)** - Guia técnico detalhado (arquitetura, patterns, exemplos)
- **[docs/](./docs/)** - Documentação adicional por tópico

---

## 🎯 Próximos Passos

1. ✅ ~~Bronze Layer implementado~~
2. ✅ ~~Silver Layer estrutura criada~~
3. ✅ ~~Transformers implementados~~
4. ⚠️ **Executar transformers e popular Silver** ← Estamos aqui
5. 🔜 Validar dados na Silver
6. 🔜 Conectar BI/Analytics
7. 🔜 Gold Layer (agregações)


[![Version](https://img.shields.io/badge/version-3.0-blue.svg)]()
[![Python](https://img.shields.io/badge/python-3.10+-blue.svg)]()
[![PostgreSQL](https://img.shields.io/badge/postgresql-15+-blue.svg)]()

</div>

